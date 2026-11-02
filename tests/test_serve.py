import json
import subprocess
import sys
import threading
import time

import duckdb
import pytest

from duckdb_agent.serve import Server
from duckdb_agent.tracing import init_traces_db
from test_stop_reasons import FakeClient, response, text, tool_use

SLOW_SQL = "SELECT count(*) FROM range(5000000000) t(i) WHERE md5(i::VARCHAR) LIKE '%zz%'"


class Output:
    """Stands in for stdout: collects protocol messages and lets a test wait for one."""

    def __init__(self):
        self.messages = []
        self._buffer = ""
        self._changed = threading.Condition()

    def write(self, data):
        with self._changed:
            self._buffer += data
            while "\n" in self._buffer:
                line, self._buffer = self._buffer.split("\n", 1)
                self.messages.append(json.loads(line))
            self._changed.notify_all()

    def flush(self):
        pass

    def wait_for(self, predicate, timeout=10):
        with self._changed:
            assert self._changed.wait_for(lambda: any(predicate(m) for m in self.messages), timeout), self.messages
            return next(m for m in self.messages if predicate(m))

    def of(self, request_id):
        return [m for m in self.messages if m.get("id") == request_id]


@pytest.fixture
def db(tmp_path):
    path = str(tmp_path / "shop.duckdb")
    conn = duckdb.connect(path)
    conn.execute("CREATE TABLE orders AS SELECT * FROM (VALUES (1, 10.0), (2, 20.0), (3, 5.0)) t(id, amount)")
    conn.close()
    return path


def server(client=None, traces=None):
    out = Output()
    return Server(None, out, client=client or FakeClient(), traces=traces), out


def test_hello_reports_protocol_and_defaults():
    srv, out = server()
    srv.handle({"id": "h", "type": "hello"})
    (hello,) = out.messages
    assert hello["protocol"] == 1
    assert hello["defaults"]["effort"] == "medium"
    assert "max" in hello["effort_levels"]


def test_run_streams_events_then_result(db, tmp_path):
    traces_path = str(tmp_path / "traces.duckdb")
    client = FakeClient(
        response("tool_use", tool_use("SELECT count(*) FROM orders")),
        response("end_turn", text("3 orders.")),
    )
    srv, out = server(client, traces=lambda: init_traces_db(traces_path))
    srv.handle({
        "id": "r1", "type": "run", "prompt": "How many orders?",
        "source": {"db": db}, "tables": ["orders"], "config": {"effort": "low"},
    })
    srv.wait_all()
    messages = out.of("r1")
    assert messages[0]["type"] == "accepted"
    assert messages[-1]["type"] == "result"
    assert messages[-1]["result"]["status"] == "completed"
    assert messages[-1]["result"]["text"] == "3 orders."
    events = [m["event"] for m in messages if m["type"] == "event"]
    assert events[0] == "run_start" and events[-1] == "run_end"
    assert "tool_call" in events
    assert duckdb.connect(traces_path).execute("SELECT status FROM agent_runs").fetchall() == [("completed",)]


def test_source_is_opened_read_only(db):
    srv, _ = server()
    conn = srv.open_source({"db": db, "schema": None, "load": [], "tpch": None})
    assert conn.execute("SELECT count(*) FROM orders").fetchone() == (3,)
    with pytest.raises(duckdb.Error, match="read-only"):
        conn.execute("DELETE FROM orders")


def test_loaded_data_is_built_once_and_shared(tmp_path):
    csv = tmp_path / "sales.csv"
    csv.write_text("id,amount\n1,10\n2,20\n")
    srv, _ = server()
    source = {"db": ":memory:", "schema": None, "load": [str(csv)], "tpch": None}
    first, second = srv.open_source(source), srv.open_source(source)
    assert first is not second
    assert second.execute("SELECT count(*) FROM sales").fetchone() == (2,)
    assert len(srv._sources) == 1


@pytest.mark.parametrize("request_fields, message", [
    ({"prompt": "", "source": {"db": "x.duckdb"}}, "prompt"),
    ({"prompt": "q"}, "source"),
    ({"prompt": "q", "source": {"db": "x.duckdb", "tpch": 0.1}}, "leave out"),
    ({"prompt": "q", "source": {"tpch": -1}}, "positive"),
    ({"prompt": "q", "source": {"db": "x.duckdb"}, "config": {"efort": "high"}}, "unknown config fields: efort"),
    ({"prompt": "q", "source": {"db": "x.duckdb"}, "config": {"effort": "extreme"}}, "invalid config"),
    ({"prompt": "q", "source": {"db": "x.duckdb"}, "tables": "orders"}, "tables"),
])
def test_bad_run_requests_get_an_error(request_fields, message):
    srv, out = server()
    srv.handle({"id": "r", "type": "run", **request_fields})
    (reply,) = out.messages
    assert reply["type"] == "error"
    assert message in reply["error"]


def test_missing_database_is_an_error_not_a_crash(tmp_path):
    srv, out = server()
    srv.handle({"id": "r", "type": "run", "prompt": "q", "source": {"load": [str(tmp_path / "nope.csv")]}})
    srv.wait_all()
    assert out.of("r")[-1]["type"] == "error"
    assert "nope.csv" in out.of("r")[-1]["error"]


def test_stop_cancels_a_running_query(db):
    client = FakeClient(response("tool_use", tool_use(SLOW_SQL)))
    srv, out = server(client)
    srv.handle({
        "id": "r1", "type": "run", "prompt": "q", "source": {"db": db},
        "config": {"query_timeout_seconds": 60},
    })
    out.wait_for(lambda m: m.get("event") == "tool_call_start")
    time.sleep(0.2)  # let the query get going
    srv.handle({"id": "s", "type": "stop", "run": "r1"})
    srv.wait_all()
    assert out.of("s") == [{"id": "s", "type": "ok"}]
    assert out.of("r1")[-1]["result"]["status"] == "cancelled"


def test_stop_unknown_run_is_an_error():
    srv, out = server()
    srv.handle({"id": "s", "type": "stop", "run": "nope"})
    assert out.messages[0]["type"] == "error"


def test_runs_in_parallel_for_ab(db):
    # Each run gets its own scripted client response; FakeClient pops in call order.
    client = FakeClient(response("end_turn", text("A")), response("end_turn", text("B")))
    srv, out = server(client)
    for run_id in ("a", "b"):
        srv.handle({"id": run_id, "type": "run", "prompt": "q", "source": {"db": db}})
    srv.wait_all()
    answers = {out.of(run_id)[-1]["result"]["text"] for run_id in ("a", "b")}
    assert answers == {"A", "B"}


def test_subprocess_speaks_only_json_on_stdout():
    requests = [
        {"id": "h", "type": "hello"},
        {"id": "x", "type": "nonsense"},
        {"id": "q", "type": "shutdown"},
    ]
    proc = subprocess.run(
        [sys.executable, "-m", "duckdb_agent.serve", "--no-traces", "--verbose"],
        input="".join(json.dumps(r) + "\n" for r in requests) + "not json\n",
        capture_output=True, text=True, timeout=60,
        env={"PATH": "", "ANTHROPIC_API_KEY": ""},  # no key needed until a run starts
    )
    assert proc.returncode == 0, proc.stderr
    replies = [json.loads(line) for line in proc.stdout.splitlines()]
    assert [(r["id"], r["type"]) for r in replies] == [("h", "hello"), ("x", "error"), ("q", "ok")]
