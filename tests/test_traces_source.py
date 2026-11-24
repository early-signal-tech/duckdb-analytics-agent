"""The traces as a run's data ({"db": "agent:traces"}), read through the traces server."""

import pytest

from duckdb_agent.follow_up import check_source
from duckdb_agent.serve import RequestError, Server, parse_source
from duckdb_agent.sources import TRACES_SOURCE
from duckdb_agent.sql import execute_query
from duckdb_agent.tools import run_tool
from duckdb_agent.traces_server import connect_traces_db, stop_traces_server
from duckdb_agent.tracing import describe_source
from test_serve import Output
from test_traces_db import free_port


@pytest.fixture
def served(tmp_path):
    """A traces server on a free port with two runs in it, and a Server using it."""
    path = str(tmp_path / "traces.duckdb")
    traces = connect_traces_db(path, port=free_port())
    traces.insert("agent_runs", {"run_id": "r1", "prompt": "Top 3?", "status": "completed", "estimated_cost_usd": 0.002})
    traces.insert("agent_runs", {"run_id": "r2", "prompt": "And 1996?", "status": "completed", "parent_run_id": "r1"})
    out = Output()
    server = Server(None, out, traces=lambda: connect_traces_db(path), traces_path=path)
    try:
        yield server, out
    finally:
        traces.close()
        stop_traces_server(path)


def source():
    return parse_source({"db": TRACES_SOURCE})


def test_hello_says_how_to_ask_about_the_traces(served, tmp_path):
    server, out = served
    server.handle({"id": "h", "type": "hello"})
    assert out.messages[-1]["traces_source"] == "agent:traces"
    off = Output()
    Server(None, off).handle({"id": "h", "type": "hello"})
    assert off.messages[-1]["traces_source"] is None


def test_a_run_reads_the_traces_through_the_server(served):
    server, _ = served
    conn = server.open_source(source())
    try:
        result = execute_query(conn, "SELECT count(*) AS runs, sum(estimated_cost_usd) AS cost FROM agent_runs")
        assert result["rows"][0][0] == 2
        tables = run_tool(conn, "list_tables", {})
        assert "agent_runs" in str(tables) and "tool_calls" in str(tables)
        described = run_tool(conn, "describe_table", {"table": "agent_runs"})
        assert "parent_run_id" in str(described)
        described_source = describe_source(conn)
        assert described_source["source_catalog"] == "source"
        assert described_source["source_db"].startswith("quack:localhost:")
    finally:
        conn.close()


def test_the_traces_stay_read_only_and_nothing_else_is_reachable(served):
    server, _ = served
    conn = server.open_source(source())
    try:
        # Quack doesn't enforce READ_ONLY, so the read-only SQL check refuses writes.
        write = execute_query(conn, "INSERT INTO agent_runs (run_id) VALUES ('x')")
        assert write.get("error_type") and "error" in write
        assert execute_query(conn, "SELECT count(*) FROM agent_runs")["rows"][0][0] == 2
        # The lock-down still blocks every other file and URL.
        for sql in ("SELECT * FROM read_csv('/etc/hosts')", "SELECT * FROM 'https://example.com/x.csv'"):
            assert execute_query(conn, sql).get("error_type"), sql
    finally:
        conn.close()


def test_a_follow_up_can_switch_to_the_traces(served):
    check_source({"tpch": 0.1}, {"db": TRACES_SOURCE})  # a database: allowed


def test_no_traces_no_traces_source(tmp_path):
    with pytest.raises(RequestError, match="traces are off"):
        Server(None, Output()).open_source(source())
    stopped = Server(None, Output(), traces_path=str(tmp_path / "nobody.duckdb"))
    with pytest.raises(RequestError, match="isn't running"):
        stopped.open_source(source())
