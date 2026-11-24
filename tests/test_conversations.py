import json

import pytest

from duckdb_agent.conversations import MAX_TURNS, conversation_turns, list_conversations
from duckdb_agent.serve import Server
from duckdb_agent.tracing import init_traces_db
from test_serve import Output


def add(traces, run_id, parent=None, labels=None, prompt="q", minutes=0, status="completed", cost=0.001,
        answer="ok", source=None):
    traces.conn.execute(
        "INSERT INTO agent_runs (run_id, parent_run_id, prompt, final_response, status, total_iterations, labels, "
        "started_at, ended_at, duration_ms, estimated_cost_usd, data_source) VALUES (?, ?, ?, ?, ?, 3, ?::JSON::VARIANT, "
        "TIMESTAMP '2026-11-23 10:00' + INTERVAL (?) MINUTE, TIMESTAMP '2026-11-23 10:00' + INTERVAL (?) MINUTE, 1200, ?, "
        "?::JSON::VARIANT)",
        [run_id, parent, prompt, answer, status, json.dumps(labels) if labels else None, minutes, minutes, cost,
         json.dumps(source) if source else None],
    )


def query(traces, run_id, n):
    for i in range(n):
        traces.conn.execute(
            "INSERT INTO tool_calls (id, run_id, tool_name, input_sql, success) VALUES (?, ?, 'query_database', 'SELECT 1', true)",
            [traces.next_id("tool_calls"), run_id],
        )


RACK = {"rack_preset": "rack-chat"}
TPCH = {"tpch": 0.1}


@pytest.fixture
def traces(tmp_path):
    t = init_traces_db(str(tmp_path / "traces.duckdb"))
    # A three-turn conversation, then a newer one-turn conversation that switched nothing.
    add(t, "n1", None, RACK, prompt="Top 3 nations in 1995?", minutes=0, source=TPCH, cost=0.002)
    add(t, "n2", "n1", RACK, prompt="And in 1996?", minutes=5, source=TPCH, cost=0.001)
    add(t, "n3", "n2", RACK, prompt="Their suppliers?", minutes=9, source={"db": "/data/shop.duckdb", "schema": None},
        status="max_iterations", cost=0.003)
    add(t, "late", None, RACK, prompt="Late shipments by month?", minutes=7, source=TPCH)
    query(t, "n1", 3)
    query(t, "n2", 1)
    # Not conversations of rack-chat: an A/B pair, a setlist run, another preset, no preset.
    add(t, "a1", None, {**RACK, "pair": "ab-1", "ab": "A"}, minutes=1)
    add(t, "s1", None, {**RACK, "setlist_run": "sl-1"}, minutes=2)
    add(t, "other", None, {"rack_preset": "rack-b"}, minutes=3)
    add(t, "plain", None, None, minutes=4)
    yield t
    t.close()


def test_a_presets_conversations_most_recently_active_first(traces):
    convos = list_conversations(traces, "rack-chat")
    assert [c["root_run_id"] for c in convos] == ["n1", "late"]
    n = convos[0]
    assert n["last_run_id"] == "n3" and n["turns"] == 3 and n["title"] == "Top 3 nations in 1995?"
    assert n["last_status"] == "max_iterations"
    assert n["cost_usd"] == pytest.approx(0.006)
    assert n["data_source"] == {"db": "/data/shop.duckdb", "schema": None}
    assert n["started_at"].startswith("2026-11-23T10:00") and n["ended_at"].startswith("2026-11-23T10:09")


def test_conversations_outside_any_preset_and_limit(traces):
    assert [c["root_run_id"] for c in list_conversations(traces, None)] == ["plain"]
    assert len(list_conversations(traces, "rack-chat", limit=1)) == 1


def test_a_conversations_turns_oldest_first(traces):
    turns = conversation_turns(traces, "n3")
    assert [t["run_id"] for t in turns] == ["n1", "n2", "n3"]
    assert [t["queries"] for t in turns] == [3, 1, 0]
    assert turns[0]["parent_run_id"] is None and turns[1]["parent_run_id"] == "n1"
    assert turns[0]["data_source"] == TPCH and turns[0]["answer"] == "ok" and turns[0]["duration_ms"] == 1200
    # Up to the run asked for, not past it.
    assert [t["run_id"] for t in conversation_turns(traces, "n2")] == ["n1", "n2"]


def test_an_unknown_run_has_no_turns_and_a_missing_parent_ends_the_walk(traces):
    assert conversation_turns(traces, "nope") == []
    add(traces, "orphan", "trimmed-away", RACK, minutes=20)
    assert [t["run_id"] for t in conversation_turns(traces, "orphan")] == ["orphan"]


def test_a_run_followed_up_twice_lists_its_newest_branch(traces):
    add(traces, "n2b", "n1", RACK, prompt="Or in 1997?", minutes=12)
    n = list_conversations(traces, "rack-chat")[0]
    assert n["root_run_id"] == "n1" and n["last_run_id"] == "n2b" and n["turns"] == 4


def test_serve_conversations_and_conversation(tmp_path, traces):
    path = str(tmp_path / "traces.duckdb")
    traces.close()
    out = Output()
    server = Server(None, out, traces=lambda: init_traces_db(path), traces_path=path)
    server.handle({"id": "c", "type": "conversations", "rack_preset": "rack-chat"})
    server.handle({"id": "t", "type": "conversation", "run_id": "n2"})
    server.handle({"id": "x", "type": "conversation"})
    convos, convo, bad = out.messages
    assert convos["type"] == "conversations" and convos["max_turns"] == MAX_TURNS == 3
    assert [c["root_run_id"] for c in convos["conversations"]] == ["n1", "late"]
    assert convo["type"] == "conversation" and [t["run_id"] for t in convo["turns"]] == ["n1", "n2"]
    assert bad["type"] == "error" and "run_id" in bad["error"]


def test_conversations_through_the_traces_server(tmp_path):
    """The app reads conversations through the Quack server: a recursive query, and no
    query parameters."""
    from duckdb_agent.traces_server import connect_traces_db, stop_traces_server
    from test_traces_db import free_port
    path = str(tmp_path / "served.duckdb")
    traces = connect_traces_db(path, port=free_port())
    try:
        assert traces.remote
        labels = {"rack_preset": "it's-mine"}
        traces.insert("agent_runs", {"run_id": "q1", "prompt": "Top 3?", "status": "completed", "labels": labels,
                                     "started_at": "2026-11-23 10:00:00", "ended_at": "2026-11-23 10:01:00"})
        traces.insert("agent_runs", {"run_id": "q2", "parent_run_id": "q1", "prompt": "And 1996?", "status": "completed",
                                     "labels": labels, "started_at": "2026-11-23 10:02:00", "ended_at": "2026-11-23 10:03:00"})
        convos = list_conversations(traces, "it's-mine")
        assert [(c["root_run_id"], c["last_run_id"], c["turns"]) for c in convos] == [("q1", "q2", 2)]
        assert [t["run_id"] for t in conversation_turns(traces, "q2")] == ["q1", "q2"]
    finally:
        traces.close()
        stop_traces_server(path)
