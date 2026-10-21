import duckdb
import pytest

import agent_harness
from duckdb_agent.log import logger
from test_stop_reasons import FakeClient, response, text, tool_use


@pytest.fixture(autouse=True)
def restore_logger():
    handlers, level, propagate = list(logger.handlers), logger.level, logger.propagate
    yield
    logger.handlers[:] = handlers
    logger.setLevel(level)
    logger.propagate = propagate


@pytest.fixture
def db(tmp_path):
    path = tmp_path / "shop.duckdb"
    conn = duckdb.connect(str(path))
    conn.execute("CREATE SCHEMA sales")
    conn.execute("CREATE TABLE sales.orders AS SELECT range AS id FROM range(3)")
    conn.close()
    return str(path)


def test_defaults_target_ducklake_prod():
    args = agent_harness.parse_args([])
    assert args.db == "ducklake:ducklake_prod"
    assert args.schema == "gold"
    assert args.tables == ["events_rich_ml_gold"]
    assert args.effort == "medium" and not args.quiet and not args.structured


def test_custom_db_has_no_default_table_or_schema(db):
    args = agent_harness.parse_args(["--db", db])
    assert args.schema is None and args.tables is None


def test_repeatable_tables_and_settings():
    args = agent_harness.parse_args(["--table", "a", "--table", "b", "--effort", "high", "--max-iterations", "5"])
    assert args.tables == ["a", "b"]
    assert (args.effort, args.max_iterations) == ("high", 5)


def test_no_table_skips_the_default_preload():
    args = agent_harness.parse_args(["--no-table"])
    assert args.tables is None
    assert args.schema == "gold"


def test_no_table_conflicts_with_table():
    with pytest.raises(SystemExit):
        agent_harness.parse_args(["--no-table", "--table", "a"])


def test_invalid_effort_is_rejected():
    with pytest.raises(SystemExit):
        agent_harness.parse_args(["--effort", "extreme"])


def test_connection_is_read_only(db):
    conn = agent_harness.connect(db, "sales")
    assert conn.execute("SELECT count(*) FROM orders").fetchone() == (3,)
    with pytest.raises(duckdb.Error, match="read-only"):
        conn.execute("DELETE FROM orders")


def test_main_runs_end_to_end(db, tmp_path, capsys):
    client = FakeClient(response("tool_use", tool_use("SELECT count(*) FROM orders")), response("end_turn", text("3 orders.")))
    traces = str(tmp_path / "traces.duckdb")
    status = agent_harness.main(
        ["--db", db, "--schema", "sales", "--table", "orders", "--traces-db", traces, "--quiet"], client=client
    )
    assert status == 0
    assert capsys.readouterr().out.strip() == "3 orders."  # --quiet prints the answer to stdout
    runs = duckdb.connect(traces).execute("SELECT status, table_name FROM agent_runs").fetchall()
    assert runs == [("completed", "orders")]


def test_main_exit_status_is_1_when_not_completed(db, tmp_path):
    client = FakeClient(response("max_tokens", text("cut off")))
    status = agent_harness.main(["--db", db, "--traces-db", str(tmp_path / "t.duckdb"), "--quiet"], client=client)
    assert status == 1


def test_load_defaults_to_in_memory_and_creates_tables(tmp_path):
    csv = tmp_path / "Sales 2026.csv"
    csv.write_text("id,amount\n1,10\n2,20\n")
    args = agent_harness.parse_args(["--load", str(csv)])
    assert args.db == ":memory:" and args.tables is None
    conn = agent_harness.connect(args.db, args.schema, args.load)
    assert conn.execute("SELECT sum(amount) FROM sales_2026").fetchone() == (30,)


def test_load_conflicts_with_a_file_db(db):
    with pytest.raises(SystemExit):
        agent_harness.parse_args(["--db", db, "--load", "x.csv"])


def test_load_missing_file_is_an_error(tmp_path):
    with pytest.raises(FileNotFoundError):
        agent_harness.connect(":memory:", None, [str(tmp_path / "nope.csv")])


def test_tpch_defaults_to_in_memory():
    args = agent_harness.parse_args(["--tpch"])
    assert args.db == ":memory:" and args.tpch == 0.1
    assert agent_harness.parse_args(["--tpch", "0.5"]).tpch == 0.5


def test_tpch_conflicts_with_a_file_db(db):
    with pytest.raises(SystemExit):
        agent_harness.parse_args(["--db", db, "--tpch"])


def test_tpch_generates_tables():
    try:
        conn = agent_harness.connect(":memory:", None, tpch=0.01)
    except duckdb.Error as e:
        pytest.skip(f"tpch extension unavailable: {e}")
    assert conn.execute("SELECT count(*) FROM customer").fetchone()[0] > 0


def test_save_db_builds_a_reusable_file(tmp_path, capsys):
    csv = tmp_path / "sales.csv"
    csv.write_text("id,amount\n1,10\n2,20\n")
    path = tmp_path / "saved.duckdb"
    assert agent_harness.main(["--load", str(csv), "--save-db", str(path), "--no-run"]) == 0
    assert "Saved database" in capsys.readouterr().err
    conn = agent_harness.connect(str(path), None)
    assert conn.execute("SELECT sum(amount) FROM sales").fetchone() == (30,)


def test_save_db_runs_against_the_saved_file(tmp_path):
    csv = tmp_path / "sales.csv"
    csv.write_text("id\n1\n")
    path = tmp_path / "saved.duckdb"
    client = FakeClient(response("end_turn", text("done")))
    args = ["--load", str(csv), "--save-db", str(path), "--traces-db", str(tmp_path / "t.duckdb"), "--quiet"]
    assert agent_harness.main(args, client=client) == 0
    assert path.exists()


def test_save_db_refuses_to_overwrite(tmp_path):
    path = tmp_path / "saved.duckdb"
    path.write_text("")
    with pytest.raises(SystemExit):
        agent_harness.parse_args(["--tpch", "--save-db", str(path)])


@pytest.mark.parametrize("argv", [
    ["--save-db", "x.duckdb"],                      # nothing to save
    ["--tpch", "--save-db", "x.duckdb", "--db", "y.duckdb"],
    ["--no-run"],
])
def test_save_db_invalid_combinations(argv):
    with pytest.raises(SystemExit):
        agent_harness.parse_args(argv)


def test_failed_build_leaves_no_file(tmp_path):
    path = tmp_path / "saved.duckdb"
    with pytest.raises(FileNotFoundError):
        agent_harness.build_database(str(path), [str(tmp_path / "missing.csv")])
    assert not path.exists()
