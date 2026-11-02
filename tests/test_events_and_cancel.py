import json
import threading
import time
from types import SimpleNamespace

import duckdb
import pytest

from duckdb_agent.config import AgentConfig
from duckdb_agent.loop import run_agent
from duckdb_agent.sql import execute_query
from duckdb_agent.tracing import init_traces_db
from test_stop_reasons import FakeClient, response, text, tool_use

SLOW_SQL = "SELECT count(*) FROM range(5000000000) t(i) WHERE md5(i::VARCHAR) LIKE '%zz%'"


@pytest.fixture
def conn():
    conn = duckdb.connect()
    conn.execute("CREATE TABLE t AS SELECT 1 AS a")
    return conn


def usage(input_tokens=100, output_tokens=20, cache_read=0, cache_write=0):
    return SimpleNamespace(
        input_tokens=input_tokens, output_tokens=output_tokens,
        cache_read_input_tokens=cache_read, cache_creation_input_tokens=cache_write,
    )


def with_usage(resp, **kwargs):
    resp.usage = usage(**kwargs)
    return resp


# --- events ---

def test_events_follow_the_run(conn):
    events = []
    client = FakeClient(
        with_usage(response("tool_use", tool_use("SELECT count(*) FROM t")), input_tokens=500, cache_read=1000),
        with_usage(response("end_turn", text("one row")), input_tokens=50),
    )
    result = run_agent(conn, "how many?", tables="t", client=client, on_event=events.append)

    names = [e["event"] for e in events]
    assert names == [
        "run_start",
        "api_call_start", "api_call", "tool_call_start", "tool_call",
        "api_call_start", "api_call",
        "run_end",
    ]
    assert all(e["run_id"] == result.run_id for e in events)
    json.dumps(events, allow_nan=False)  # every event is plain JSON

    first_call = events[2]
    assert first_call["iteration"] == 1
    assert first_call["context_tokens"] == 1500  # input + cache read + cache write
    assert first_call["usage"]["cache_read_input_tokens"] == 1000

    tool_call = events[4]
    assert tool_call["tool"] == "query_database"
    assert tool_call["success"] is True
    assert tool_call["row_count"] == 1
    assert tool_call["tables"] == ["main.t"]

    end = events[-1]
    assert end["status"] == "completed"
    assert end["text"] == "one row"
    assert end["usage"]["input_tokens"] == 550
    assert end["api_calls"] == 2


def test_run_start_carries_the_config(conn):
    events = []
    run_agent(conn, "q", client=FakeClient(response("end_turn", text("x"))),
              config=AgentConfig(effort="high"), on_event=events.append)
    start = events[0]
    assert start["effort"] == "high"
    assert start["config"]["effort"] == "high"
    assert start["config"]["system_prompt"].startswith("sha256:")


def test_thinking_and_context_edits_are_events(conn):
    events = []
    resp = response("end_turn", SimpleNamespace(type="thinking", thinking="Let me look."), text("done"))
    resp.context_management = SimpleNamespace(applied_edits=[SimpleNamespace(type="clear_tool_uses_20250919", cleared_input_tokens=24000)])
    run_agent(conn, "q", client=FakeClient(resp), on_event=events.append)
    by_name = {e["event"]: e for e in events}
    assert by_name["thinking"]["text"] == "Let me look."
    assert by_name["context_edit"]["details"]["cleared_input_tokens"] == 24000


def test_a_failing_callback_does_not_stop_the_run(conn):
    def explode(event):
        raise RuntimeError("UI went away")

    result = run_agent(conn, "q", client=FakeClient(response("end_turn", text("fine"))), on_event=explode)
    assert result.status == "completed"
    assert result.text == "fine"


# --- cancel ---

def test_cancel_before_start_makes_no_api_call(conn, tmp_path):
    cancel = threading.Event()
    cancel.set()
    client = FakeClient()
    traces = init_traces_db(str(tmp_path / "traces.duckdb"))
    events = []
    result = run_agent(conn, "q", client=client, cancel=cancel, traces_conn=traces, on_event=events.append)
    assert result.status == "cancelled"
    assert result.iterations == 0
    assert result.api_calls == 0
    assert events[-1]["event"] == "run_end" and events[-1]["status"] == "cancelled"
    assert traces.execute("SELECT status FROM agent_runs").fetchall() == [("cancelled",)]


def test_cancel_during_api_call_skips_its_tools(conn):
    cancel = threading.Event()
    client = FakeClient(response("tool_use", tool_use("SELECT count(*) FROM t")))
    original_create = client.create

    def create_then_cancel(**kwargs):
        cancel.set()  # the user pressed STOP while Claude was answering
        return original_create(**kwargs)

    client.beta.messages.create = create_then_cancel
    events = []
    result = run_agent(conn, "q", client=client, cancel=cancel, on_event=events.append)
    assert result.status == "cancelled"
    assert result.iterations == 1
    assert "tool_call_start" not in [e["event"] for e in events]


def test_cancel_interrupts_a_running_query(conn):
    cancel = threading.Event()
    client = FakeClient(response("tool_use", tool_use(SLOW_SQL)))
    events = []
    threading.Timer(0.3, cancel.set).start()
    start = time.monotonic()
    result = run_agent(conn, "q", client=client, cancel=cancel, on_event=events.append,
                       config=AgentConfig(query_timeout_seconds=60))
    assert time.monotonic() - start < 10
    assert result.status == "cancelled"
    tool_call = next(e for e in events if e["event"] == "tool_call")
    assert tool_call["error_type"] == "cancelled"


def test_execute_query_reports_cancel_and_connection_still_works(conn):
    cancel = threading.Event()
    threading.Timer(0.3, cancel.set).start()
    result = execute_query(conn, SLOW_SQL, timeout_seconds=None, cancel=cancel)
    assert result["success"] is False
    assert result["error_type"] == "cancelled"
    assert execute_query(conn, "SELECT 42 AS x", cancel=threading.Event())["rows"] == [[42]]


def test_query_skipped_when_already_cancelled(conn):
    cancel = threading.Event()
    cancel.set()
    assert execute_query(conn, "SELECT 1", cancel=cancel)["error_type"] == "cancelled"


def test_timeout_still_reported_as_timeout_with_cancel_watching(conn):
    result = execute_query(conn, SLOW_SQL, timeout_seconds=0.3, cancel=threading.Event())
    assert result["error_type"] == "timeout"


def test_run_start_carries_preset_and_labels(conn):
    events = []
    run_agent(conn, "q", client=FakeClient(response("end_turn", text("x"))), on_event=events.append,
              preset={"name": "Quick", "revision": 1}, labels={"question": "Q1"})
    assert events[0]["preset"] == {"name": "Quick", "revision": 1}
    assert events[0]["labels"] == {"question": "Q1"}
