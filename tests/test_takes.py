import json

import pytest

from duckdb_agent.serve import Server
from duckdb_agent.takes import list_takes, take_runs
from duckdb_agent.tracing import init_traces_db
from test_serve import Output


def add(traces, run_id, labels=None, prompt="q", minutes=0, status="completed", cost=0.001, answer="ok"):
    traces.conn.execute(
        "INSERT INTO agent_runs (run_id, prompt, final_response, status, total_iterations, labels, started_at, ended_at, "
        "estimated_cost_usd) VALUES (?, ?, ?, ?, 3, ?::JSON::VARIANT, now() + INTERVAL (?) MINUTE, now() + INTERVAL (?) MINUTE, ?)",
        [run_id, prompt, answer, status, json.dumps(labels) if labels else None, minutes, minutes, cost],
    )


@pytest.fixture
def traces(tmp_path):
    t = init_traces_db(str(tmp_path / "traces.duckdb"))
    rack = {"rack_preset": "rack-a", "rack_edited": False}
    add(t, "r1", rack, prompt="Top 5?", minutes=0)
    add(t, "a1", {**rack, "pair": "ab-1", "ab": "A"}, prompt="Compare?", minutes=1)
    add(t, "b1", {**rack, "pair": "ab-1", "ab": "B"}, prompt="Compare?", minutes=1, status="max_iterations")
    for i, q in enumerate(["q1", "q2"]):
        add(t, f"s{i}", {**rack, "setlist_run": "sl-1", "setlist": "tpch", "setlist_name": "Nightly", "question": q,
                         "column": "A", "repeat": 1}, minutes=2)
    add(t, "other", {"rack_preset": "rack-b"}, minutes=3)
    add(t, "plain", None, minutes=4)
    yield t
    t.close()


def test_a_presets_takes_newest_first(traces):
    takes = list_takes(traces, "rack-a")
    assert [(t["kind"], t["key"], t["runs"], t["completed"]) for t in takes] == [
        ("setlist", "sl-1", 2, 2), ("ab", "ab-1", 2, 1), ("run", "r1", 1, 1)]
    setlist, ab, run = takes
    assert setlist["prompt"] is None and setlist["setlist_name"] == "Nightly" and setlist["question_set"] == "tpch"
    assert ab["prompt"] == "Compare?" and run["prompt"] == "Top 5?"
    assert run["cost_usd"] == pytest.approx(0.001) and ab["cost_usd"] == pytest.approx(0.002)


def test_takes_outside_any_preset(traces):
    assert [t["key"] for t in list_takes(traces, None)] == ["plain"]


def test_a_takes_runs_in_order(traces):
    assert [r["run_id"] for r in take_runs(traces, "ab-1")] == ["a1", "b1"]
    runs = take_runs(traces, "sl-1")
    assert [r["labels"]["question"] for r in runs] == ["q1", "q2"]
    assert take_runs(traces, "r1")[0]["answer"] == "ok"


def test_serve_takes_and_take(tmp_path, traces):
    path = str(tmp_path / "traces.duckdb")
    traces.close()
    out = Output()
    server = Server(None, out, traces=lambda: init_traces_db(path), traces_path=path)
    server.handle({"id": "k", "type": "takes", "rack_preset": "rack-a", "limit": 2})
    server.handle({"id": "t", "type": "take", "key": "ab-1"})
    server.handle({"id": "x", "type": "take"})
    takes, take, bad = out.messages
    assert takes["type"] == "takes" and [t["key"] for t in takes["takes"]] == ["sl-1", "ab-1"]
    assert take["type"] == "take" and [r["labels"]["ab"] for r in take["runs"]] == ["A", "B"]
    assert bad["type"] == "error" and "key" in bad["error"]


def test_takes_through_the_traces_server(tmp_path):
    """The app reads takes through the Quack server, which doesn't forward query parameters."""
    from duckdb_agent.traces_server import connect_traces_db, stop_traces_server
    from test_traces_db import free_port
    path = str(tmp_path / "served.duckdb")
    traces = connect_traces_db(path, port=free_port())
    try:
        assert traces.remote
        traces.insert("agent_runs", {"run_id": "r1", "prompt": "q", "status": "completed",
                                     "labels": {"rack_preset": "it's-mine"}})
        traces.insert("agent_runs", {"run_id": "a", "prompt": "q", "status": "completed",
                                     "labels": {"rack_preset": "it's-mine", "pair": "ab-1", "ab": "A"}})
        assert sorted(t["key"] for t in list_takes(traces, "it's-mine")) == ["ab-1", "r1"]  # a quote in an id is fine
        assert [r["run_id"] for r in take_runs(traces, "ab-1")] == ["a"]
        assert list_takes(traces, None) == []
    finally:
        traces.close()
        stop_traces_server(path)
