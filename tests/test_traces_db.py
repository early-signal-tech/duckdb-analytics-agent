import socket
from types import SimpleNamespace

import duckdb
import pytest

from duckdb_agent import connect_traces_db, stop_traces_server
from duckdb_agent.loop import run_agent
from duckdb_agent import traces_server
from duckdb_agent.traces_server import TracesServerError, server_status, state_paths
from duckdb_agent.tracing import TracesSchemaError, describe_source, init_traces_db
from test_stop_reasons import FakeClient, response, text, tool_use


@pytest.fixture
def conn():
    conn = duckdb.connect()
    conn.execute("CREATE TABLE t AS SELECT 1 AS a")
    return conn


@pytest.fixture
def traces_conn(tmp_path):
    return init_traces_db(str(tmp_path / "traces.duckdb"))


def run_query(conn, traces_conn, sql):
    client = FakeClient(response("tool_use", tool_use(sql)), response("end_turn", text("ok")))
    return run_agent(conn, "analyze", tables="t", client=client, traces_conn=traces_conn)


def free_port():
    with socket.socket() as s:
        s.bind(("localhost", 0))
        return s.getsockname()[1]


# --- schema ---

def test_older_schema_is_refused(tmp_path):
    path = str(tmp_path / "old.duckdb")
    old = duckdb.connect(path)
    old.execute("CREATE TABLE agent_runs (run_id VARCHAR PRIMARY KEY, config JSON)")
    old.close()
    with pytest.raises(TracesSchemaError, match="--reset-traces"):
        init_traces_db(path)


def test_reopening_a_current_file_works(tmp_path):
    path = str(tmp_path / "traces.duckdb")
    init_traces_db(path).close()
    init_traces_db(path).close()


# --- VARIANT columns ---

def test_tool_input_and_result_are_queryable_variants(conn, traces_conn):
    result = run_query(conn, traces_conn, "SELECT a FROM t")
    row = traces_conn.execute("""
        SELECT tool_input.sql::VARCHAR, query_result.success::BOOLEAN, query_result.rows[1][1]::INTEGER
        FROM agent_traces WHERE run_id = ? AND action = 'tool_call'
    """, [result.run_id]).fetchone()
    assert row == ("SELECT a FROM t", True, 1)


def test_schema_lookup_logs_the_table_not_fake_sql(conn, traces_conn):
    result = run_query(conn, traces_conn, "SELECT 1")
    sql, table = traces_conn.execute("""
        SELECT sql_query, tool_input.table::VARCHAR FROM agent_traces
        WHERE run_id = ? AND action = 'schema_lookup'
    """, [result.run_id]).fetchone()
    assert (sql, table) == (None, "t")


def test_only_query_database_fills_input_sql(conn, traces_conn):
    client = FakeClient(
        response("tool_use", SimpleNamespace(type="tool_use", id="toolu_1", name="describe_table", input={"table": "t"})),
        response("end_turn", text("ok")),
    )
    result = run_agent(conn, "analyze", client=client, traces_conn=traces_conn)
    row = traces_conn.execute(
        "SELECT input_sql, tool_input FROM tool_calls WHERE run_id = ?", [result.run_id]
    ).fetchone()
    assert row == (None, {"table": "t"})


# --- run_start and the replay script ---

def test_run_start_records_the_source(tmp_path, traces_conn):
    db = str(tmp_path / "data.duckdb")
    duckdb.connect(db).execute("CREATE SCHEMA sales; CREATE TABLE sales.t AS SELECT 1 AS a").close()
    conn = duckdb.connect()
    conn.execute(f"ATTACH '{db}' AS source (READ_ONLY)")
    conn.execute("USE source.sales")
    result = run_query(conn, traces_conn, "SELECT a FROM t")
    start = traces_conn.execute(
        "SELECT details FROM agent_traces WHERE run_id = ? AND action = 'run_start'", [result.run_id]
    ).fetchone()[0]
    assert (start["source_db"], start["source_catalog"], start["source_schema"]) == (db, "source", "sales")
    assert traces_conn.execute(
        "SELECT source_db FROM agent_runs WHERE run_id = ?", [result.run_id]
    ).fetchone()[0] == db


def test_replay_script_runs_in_a_fresh_session(tmp_path, traces_conn):
    db = str(tmp_path / "data.duckdb")
    duckdb.connect(db).execute("CREATE SCHEMA sales; CREATE TABLE sales.t AS SELECT 42 AS a").close()
    conn = duckdb.connect()
    conn.execute(f"ATTACH '{db}' AS source (READ_ONLY)")
    conn.execute("USE source.sales")
    result = run_query(conn, traces_conn, "SELECT a FROM t -- the answer\n")
    script = traces_conn.execute("SELECT script FROM replay_sql WHERE run_id = ?", [result.run_id]).fetchone()[0]
    assert script == (
        f"ATTACH '{db}' AS \"source\" (READ_ONLY);\n"
        'USE "source"."sales";\n'
        "SELECT a FROM t -- the answer\n;"
    )
    conn.close()
    # What you'd paste into another terminal.
    fresh = duckdb.connect()
    for statement in fresh.extract_statements(script):
        last = fresh.execute(statement)
    assert last.fetchall() == [(42,)]


def test_replay_script_flags_in_memory_sources(conn, traces_conn):
    result = run_query(conn, traces_conn, "SELECT a FROM t;")
    script = traces_conn.execute("SELECT script FROM replay_sql WHERE run_id = ?", [result.run_id]).fetchone()[0]
    assert script.splitlines() == [
        "-- The source was an in-memory database: load the same data first.",
        'USE "memory"."main";',
        "SELECT a FROM t;",
    ]


def test_describe_source_of_an_in_memory_database():
    conn = duckdb.connect()
    assert describe_source(conn) == {"source_db": None, "source_catalog": "memory", "source_schema": "main"}


# --- Quack server ---

@pytest.fixture
def served(tmp_path):
    path = str(tmp_path / "traces.duckdb")
    yield path, free_port()
    stop_traces_server(path)


def test_runs_write_through_the_server_while_others_read(conn, served):
    path, port = served
    traces = connect_traces_db(path, port=port)
    assert traces.remote
    result = run_query(conn, traces, "SELECT a FROM t")

    # Another process can't open the file, but can read it through the server.
    with pytest.raises(duckdb.IOException, match="lock"):
        duckdb.connect(path, read_only=True)
    state = server_status(path)
    reader = duckdb.connect()
    reader.execute(f"ATTACH '{state['uri']}' AS traces (TYPE quack, TOKEN '{state['token']}')")
    # Quack doesn't forward query parameters, so remote reads use literals.
    rows = reader.execute(
        f"SELECT action FROM traces.agent_traces WHERE run_id = '{result.run_id}' ORDER BY id"
    ).fetchall()
    assert rows == [("run_start",), ("schema_lookup",), ("tool_call",), ("claude_response",)]
    assert reader.execute("SELECT status FROM traces.agent_runs").fetchall() == [("completed",)]
    assert reader.execute("SELECT count(*) FROM traces.replay_sql").fetchone()[0] == 1


def test_second_client_reuses_the_running_server(served):
    path, port = served
    first = connect_traces_db(path, port=port)
    pid = server_status(path)["pid"]
    second = connect_traces_db(path, port=port)
    assert second.remote and server_status(path)["pid"] == pid
    first.close()
    second.close()


def test_stopping_the_server_releases_the_file(conn, served):
    path, port = served
    traces = connect_traces_db(path, port=port)
    run_query(conn, traces, "SELECT 1")
    traces.close()
    assert stop_traces_server(path) is True
    assert not state_paths(path)["json"].exists()
    assert duckdb.connect(path).execute("SELECT count(*) FROM agent_runs").fetchone()[0] == 1


def test_state_files_are_private(served):
    path, port = served
    connect_traces_db(path, port=port).close()
    for name in ("json", "sql"):
        assert state_paths(path)[name].stat().st_mode & 0o077 == 0


def test_falls_back_to_the_file_when_the_server_cant_start(tmp_path, caplog, monkeypatch):
    def fail(*args):
        raise TracesServerError("no server today")
    monkeypatch.setattr(traces_server, "start_traces_server", fail)
    path = str(tmp_path / "traces.duckdb")
    traces = connect_traces_db(path)
    assert not traces.remote
    assert "no server today" in caplog.text and "directly" in caplog.text
    traces.close()


def test_server_that_fails_to_start_reports_why(tmp_path):
    path = str(tmp_path / "traces.duckdb")
    holder = duckdb.connect(path)  # holds the file lock, so the server can't open it
    try:
        with pytest.raises(TracesServerError, match="exited"):
            traces_server.start_traces_server(path, free_port())
    finally:
        holder.close()
