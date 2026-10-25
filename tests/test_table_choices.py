import socket
from types import SimpleNamespace

import duckdb
import pytest

from duckdb_agent import connect_traces_db, stop_traces_server
from duckdb_agent.config import AgentConfig
from duckdb_agent.loop import run_agent
from duckdb_agent.tools import referenced_tables, tables_touched
from duckdb_agent.tracing import init_traces_db
from test_stop_reasons import FakeClient, response, text


def call(name, id="toolu_1", **tool_input):
    return SimpleNamespace(type="tool_use", id=id, name=name, input=tool_input)


@pytest.fixture
def conn():
    conn = duckdb.connect()
    conn.execute("""
        CREATE SCHEMA gold;
        CREATE TABLE orders AS SELECT 1 AS id, 10 AS amount;
        CREATE TABLE orders_old AS SELECT 1 AS id, 9 AS amount;
        CREATE TABLE gold.orders AS SELECT 1 AS id, 11 AS amount;
        CREATE TABLE customers AS SELECT 1 AS id;
    """)
    return conn


@pytest.fixture
def traces_conn(tmp_path):
    return init_traces_db(str(tmp_path / "traces.duckdb"))


def choices(traces_conn, run_id):
    return {
        row[0]: row[1:] for row in traces_conn.execute("""
            SELECT table_name, checked, queries, failed_queries, in_answer, dropped
            FROM table_choices WHERE run_id = ? ORDER BY table_name
        """, [run_id]).fetchall()
    }


# --- finding the tables a query reads ---

@pytest.mark.parametrize("sql, tables", [
    ("SELECT * FROM orders", ["main.orders"]),
    ("SELECT * FROM gold.orders o JOIN customers c ON o.id = c.id", ["gold.orders", "main.customers"]),
    ('SELECT * FROM "ORDERS"', ["main.orders"]),
    ("SELECT * FROM memory.gold.orders", ["gold.orders"]),
    ("WITH recent AS (SELECT * FROM orders_old) SELECT * FROM recent", ["main.orders_old"]),
    ("SELECT * FROM orders WHERE id IN (SELECT id FROM customers)", ["main.customers", "main.orders"]),
    ("FROM orders UNION ALL FROM orders_old", ["main.orders", "main.orders_old"]),
    ("SELECT * FROM missing_table", ["missing_table"]),
    ("SELECT 42", []),
])
def test_referenced_tables(conn, sql, tables):
    assert referenced_tables(conn, sql) == tables


def test_referenced_tables_never_runs_the_query(conn, tmp_path):
    # Binding would open the file; parsing must not.
    sql = f"SELECT * FROM read_csv('{tmp_path / 'does_not_exist.csv'}'), orders"
    assert referenced_tables(conn, sql) == ["main.orders"]


def test_unparseable_sql_has_no_tables(conn):
    assert referenced_tables(conn, "SELEC 1") is None


def test_tables_touched_per_tool(conn):
    listed = {"success": True, "rows": [["gold", "orders"], ["main", "customers"]]}
    assert tables_touched(conn, "list_tables", {}, listed) == ["gold.orders", "main.customers"]
    assert tables_touched(conn, "describe_table", {"table": "orders"}, {"success": True, "table": "main.orders"}) == ["main.orders"]
    # A table that doesn't exist was still what the agent went looking for.
    assert tables_touched(conn, "sample_rows", {"table": '"Nope"'}, {"success": False}) == ["nope"]
    assert tables_touched(conn, "query_database", {"sql": "SELECT * FROM orders"}, {"success": True}) == ["main.orders"]
    assert tables_touched(conn, "submit_answer", {}, {"success": True}) is None


# --- logging ---

def test_tool_calls_log_their_tables(conn, traces_conn):
    client = FakeClient(
        response("tool_use", call("list_tables"),
                 call("describe_table", id="toolu_2", table="orders_old"),
                 call("query_database", id="toolu_3", sql="SELECT sum(amount) FROM orders")),
        response("end_turn", text("10")),
    )
    result = run_agent(conn, "total?", client=client, traces_conn=traces_conn)
    rows = traces_conn.execute(
        "SELECT tool_name, table_names FROM tool_calls WHERE run_id = ? ORDER BY id", [result.run_id]
    ).fetchall()
    assert rows == [
        ("List Tables", ["gold.orders", "main.customers", "main.orders", "main.orders_old"]),
        ("Describe Table", ["main.orders_old"]),
        ("Query Database", ["main.orders"]),
    ]


def test_checked_then_dropped_tables_are_flagged(conn, traces_conn):
    # Preloaded orders, looked at orders_old and gold.orders, answered from orders.
    client = FakeClient(
        response("tool_use", call("describe_table", table="orders_old"),
                 call("sample_rows", id="toolu_2", table="gold.orders")),
        response("tool_use", call("query_database", id="toolu_3", sql="SELECT amount FROM orders_old"),
                 call("query_database", id="toolu_4", sql="SELECT nope FROM customers")),
        response("tool_use", call("query_database", id="toolu_5", sql="SELECT sum(amount) FROM orders")),
        response("end_turn", text("10")),
    )
    result = run_agent(conn, "total?", tables="orders", client=client, traces_conn=traces_conn)
    assert traces_conn.execute(
        "SELECT answer_tables, answer_tables_from FROM agent_runs WHERE run_id = ?", [result.run_id]
    ).fetchone() == (["main.orders"], "last_query")
    # table: (checked, queries, failed_queries, in_answer, dropped)
    assert choices(traces_conn, result.run_id) == {
        "gold.orders": (True, 0, 0, False, True),
        "main.customers": (False, 0, 1, False, True),
        "main.orders": (True, 1, 0, True, False),
        "main.orders_old": (True, 1, 0, False, True),
    }


def test_structured_answer_tables_come_from_supporting_sql(conn, traces_conn):
    answer = {
        "summary": "Total is 21.", "key_findings": ["21"], "caveats": [],
        "supporting_sql": ["SELECT sum(amount) FROM orders", "SELECT sum(amount) FROM gold.orders"],
    }
    client = FakeClient(
        response("tool_use", call("query_database", sql="SELECT 1 FROM customers")),
        response("tool_use", call("submit_answer", id="toolu_2", **answer)),
    )
    result = run_agent(conn, "total?", client=client, traces_conn=traces_conn,
                       config=AgentConfig(structured_answer=True))
    assert traces_conn.execute(
        "SELECT answer_tables, answer_tables_from FROM agent_runs WHERE run_id = ?", [result.run_id]
    ).fetchone() == (["gold.orders", "main.orders"], "supporting_sql")
    assert choices(traces_conn, result.run_id)["main.customers"][-2:] == (False, True)


def test_runs_without_an_answer_have_no_answer_tables(conn, traces_conn):
    client = FakeClient(
        response("tool_use", call("query_database", sql="SELECT 1 FROM orders")),
        response("refusal", text("")),
    )
    result = run_agent(conn, "total?", client=client, traces_conn=traces_conn)
    assert traces_conn.execute(
        "SELECT answer_tables FROM agent_runs WHERE run_id = ?", [result.run_id]
    ).fetchone() == (None,)
    # Queried, no answer to compare against: kept, not dropped.
    assert choices(traces_conn, result.run_id)["main.orders"][-2:] == (None, False)


def test_listing_alone_is_not_checking(conn, traces_conn):
    client = FakeClient(response("tool_use", call("list_tables")), response("end_turn", text("ok")))
    result = run_agent(conn, "what's here?", client=client, traces_conn=traces_conn)
    assert not any(row[-1] for row in choices(traces_conn, result.run_id).values())


# --- schema ---

def test_new_columns_are_added_to_an_existing_file(tmp_path):
    path = str(tmp_path / "traces.duckdb")
    old = init_traces_db(path)
    old.execute("ALTER TABLE tool_calls DROP COLUMN table_names")
    old.execute("ALTER TABLE agent_runs DROP COLUMN answer_tables")
    old.close()
    traces = init_traces_db(path)
    assert "table_names" in {row[0] for row in traces.execute("DESCRIBE tool_calls").fetchall()}
    assert "answer_tables" in {row[0] for row in traces.execute("DESCRIBE agent_runs").fetchall()}
    traces.execute("SELECT * FROM table_choices").fetchall()


def test_table_choices_through_the_server(conn, tmp_path):
    path = str(tmp_path / "traces.duckdb")
    with socket.socket() as s:
        s.bind(("localhost", 0))
        port = s.getsockname()[1]
    try:
        traces = connect_traces_db(path, port=port)
        assert traces.remote
        client = FakeClient(
            response("tool_use", call("describe_table", table="orders_old"),
                     call("query_database", id="toolu_2", sql="SELECT sum(amount) FROM orders")),
            response("end_turn", text("10")),
        )
        run_agent(conn, "total?", client=client, traces_conn=traces)
        dropped = traces.execute("SELECT table_name FROM table_choices WHERE dropped").fetchall()
        assert dropped == [("main.orders_old",)]
        traces.close()
    finally:
        stop_traces_server(path)
