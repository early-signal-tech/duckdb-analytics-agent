import duckdb
import pytest

import agent_harness
from duckdb_agent.config import AgentConfig
from duckdb_agent.follow_up import MAX_TURNS, FollowUpError, load_thread, source_from_data_source
from duckdb_agent.loop import run_agent
from duckdb_agent.serve import Server
from duckdb_agent.tracing import init_traces_db
from test_harness import restore_logger  # noqa: F401  (autouse: the CLI test configures logging)
from test_serve import Output
from test_stop_reasons import FakeClient, response, text, tool_use

DB = {"db": "shop.duckdb", "schema": "sales"}


@pytest.fixture
def conn():
    conn = duckdb.connect()
    conn.execute("CREATE TABLE orders AS SELECT range AS id, range * 10 AS amount FROM range(5)")
    return conn


@pytest.fixture
def traces(tmp_path):
    t = init_traces_db(str(tmp_path / "traces.duckdb"))
    yield t
    t.close()


def ask(conn, traces, prompt, *responses, parent=None, data_source=DB, **config):
    client = FakeClient(*responses)
    result = run_agent(conn, prompt, traces_conn=traces, client=client, config=AgentConfig(**config),
                       data_source=data_source, parent_run_id=parent)
    return result, client


def answered(conn, traces, prompt="Total revenue?", answer="Revenue is 100.", parent=None, data_source=DB):
    result, _ = ask(conn, traces, prompt,
                    response("tool_use", tool_use("SELECT sum(amount) FROM orders")),
                    response("end_turn", text(answer)), parent=parent, data_source=data_source)
    return result.run_id


def first_message(client) -> str:
    return client.sent_messages[0][0]["content"]


def test_a_follow_up_gets_the_earlier_turn_as_a_digest(conn, traces):
    parent = answered(conn, traces)
    result, client = ask(conn, traces, "And per order?", response("end_turn", text("20 per order.")), parent=parent)
    assert result.status == "completed"
    message = first_message(client)
    assert message.index("<previous_turn") < message.index("<question>\nAnd per order?")
    assert "<question>\nTotal revenue?\n</question>" in message
    assert '<answer status="completed">\nRevenue is 100.\n</answer>' in message
    assert "SELECT sum(amount) FROM orders;" in message and "the last query that ran" in message
    assert "<answer_tables>main.orders</answer_tables>" in message
    assert "<data_change>" not in message

    row = traces.execute(f"SELECT parent_run_id, prompt FROM agent_runs WHERE run_id = '{result.run_id}'").fetchone()
    assert row == (parent, "And per order?")  # the user's words, not the digest
    start, context = traces.execute(f"""
        SELECT details.parent_run_id::VARCHAR, (SELECT details::JSON FROM agent_traces
            WHERE run_id = '{result.run_id}' AND action = 'follow_up_context')
        FROM agent_traces WHERE run_id = '{result.run_id}' AND action = 'run_start'
    """).fetchone()
    assert start == parent
    assert '"source_changed":false' in context.replace(" ", "") and parent in context


def test_a_new_question_has_no_parent(conn, traces):
    run_id = answered(conn, traces)
    assert traces.execute(f"SELECT parent_run_id FROM agent_runs WHERE run_id = '{run_id}'").fetchone() == (None,)


def test_a_structured_answer_cites_its_supporting_sql(conn, traces):
    answer = {"summary": "Revenue is 100.", "key_findings": ["100"], "caveats": [],
              "supporting_sql": ["SELECT sum(amount) AS revenue FROM orders"]}
    submit = response("tool_use", type("B", (), {"type": "tool_use", "id": "t2", "name": "submit_answer", "input": answer})())
    parent, _ = ask(conn, traces, "Total revenue?",
                    response("tool_use", tool_use("SELECT count(*) FROM orders")), submit, structured_answer=True)
    assert parent.status == "completed"
    _, client = ask(conn, traces, "And per order?", response("end_turn", text("20.")), parent=parent.run_id)
    message = first_message(client)
    assert "SELECT sum(amount) AS revenue FROM orders;" in message and "the SQL the answer cited" in message
    assert "count(*)" not in message


def test_a_conversation_holds_at_most_three_questions(conn, traces):
    assert MAX_TURNS == 3
    first = answered(conn, traces, "Q1?", "A1.")
    second = answered(conn, traces, "Q2?", "A2.", parent=first)
    third, client = ask(conn, traces, "Q3?", response("end_turn", text("A3.")), parent=second)
    message = first_message(client)
    assert message.index("Q1?") < message.index("Q2?") < message.index("Q3?")
    assert [t["run_id"] for t in load_thread(traces, second)] == [first, second]

    runs_before = traces.execute("SELECT count(*) FROM agent_runs").fetchone()
    with pytest.raises(FollowUpError, match="already has 3 questions"):
        ask(conn, traces, "Q4?", parent=third.run_id)
    assert traces.execute("SELECT count(*) FROM agent_runs").fetchone() == runs_before  # nothing traced


def test_an_unknown_or_unfinished_parent_is_refused(conn, traces):
    with pytest.raises(FollowUpError, match="no finished run"):
        ask(conn, traces, "And?", parent="nope")
    traces.insert("agent_traces", {"run_id": "running", "iteration": 0, "action": "run_start"})
    with pytest.raises(FollowUpError, match="no finished run"):
        ask(conn, traces, "And?", parent="running")
    with pytest.raises(FollowUpError, match="no finished run"):
        ask(conn, traces, "And?", parent="it's quoted")  # inlined as a literal safely


def test_a_follow_up_needs_traces(conn):
    with pytest.raises(FollowUpError, match="traces"):
        run_agent(conn, "And?", client=FakeClient(), parent_run_id="r1")


def test_a_failed_parent_still_counts_with_its_status(conn, traces):
    result, _ = ask(conn, traces, "Total?", response("max_tokens", text("cut off")))
    _, client = ask(conn, traces, "Try again?", response("end_turn", text("100.")), parent=result.run_id)
    assert '<answer status="max_tokens">\ncut off\n</answer>' in first_message(client)
    assert "<answer_sql" not in first_message(client)


def test_a_follow_up_can_switch_to_another_database(conn, traces):
    parent = answered(conn, traces)
    other = {"db": "ducklake:warehouse"}
    _, client = ask(conn, traces, "Same in the warehouse?", response("end_turn", text("ok")), parent=parent, data_source=other)
    message = first_message(client)
    assert "<data_change>The previous turn used shop.duckdb (schema sales); this question uses ducklake:warehouse." in message


@pytest.mark.parametrize("hop", [{"load": ["sales.csv"]}, {"tpch": 0.1}, None])
def test_a_follow_up_cannot_hop_to_loaded_files_or_tpch(conn, traces, hop):
    parent = answered(conn, traces)
    with pytest.raises(FollowUpError, match="not to loaded files or TPC-H"):
        ask(conn, traces, "And?", parent=parent, data_source=hop)


def test_staying_on_loaded_files_is_fine(conn, traces):
    files = {"load": ["sales.csv"]}
    parent = answered(conn, traces, data_source=files)
    result, _ = ask(conn, traces, "And?", response("end_turn", text("ok")), parent=parent, data_source=files)
    assert result.status == "completed"


def test_a_library_caller_is_on_its_attached_database(tmp_path, traces):
    path = str(tmp_path / "shop.duckdb")
    duckdb.connect(path).execute("CREATE TABLE orders AS SELECT 1 AS id").close()
    conn = duckdb.connect()
    conn.execute(f"ATTACH '{path}' AS shop (READ_ONLY)")
    conn.execute("USE shop")
    parent = answered(conn, traces, data_source={"db": path})
    result, _ = ask(conn, traces, "And?", response("end_turn", text("ok")), parent=parent, data_source=None)
    assert result.status == "completed"


def test_source_from_data_source():
    assert source_from_data_source(DB) == DB
    assert source_from_data_source({"db": "x.duckdb"}) == {"db": "x.duckdb", "schema": None}
    assert source_from_data_source({"load": ["a.csv"]}) == {"load": ["a.csv"]}
    assert source_from_data_source({"tpch": 0.1}) == {"tpch": 0.1}
    assert source_from_data_source(None) is None


# --- the app protocol ---

def serve(tmp_path, client):
    path = str(tmp_path / "traces.duckdb")
    out = Output()
    return Server(None, out, client=client, traces=lambda: init_traces_db(path), traces_path=path), out, path


def test_serve_follow_up_runs_on_the_parents_data(tmp_path):
    client = FakeClient(response("end_turn", text("Five orders.")), response("end_turn", text("Ten each.")))
    server, out, path = serve(tmp_path, client)
    server.handle({"id": "r1", "type": "run", "prompt": "How many orders?", "source": {"tpch": 0.01}})
    server.wait_all()
    parent = out.messages[-1]["result"]["run_id"]
    server.handle({"id": "r2", "type": "run", "prompt": "And per customer?", "parent_run_id": parent})
    server.wait_all()
    result = out.messages[-1]
    assert result["type"] == "result" and result["result"]["status"] == "completed"
    assert "How many orders?" in first_message_of(client, 1)
    traces = init_traces_db(path)
    try:
        row = traces.execute(f"SELECT parent_run_id, data_source::JSON::VARCHAR FROM agent_runs "
                             f"WHERE run_id = '{result['result']['run_id']}'").fetchone()
    finally:
        traces.close()
    assert row[0] == parent and '"tpch":0.01' in row[1].replace(" ", "")


def first_message_of(client, call):
    return client.sent_messages[call][0]["content"]


def test_serve_refuses_a_follow_up_that_breaks_the_rules(tmp_path):
    client = FakeClient(response("end_turn", text("Five orders.")))
    server, out, _ = serve(tmp_path, client)
    server.handle({"id": "r1", "type": "run", "prompt": "How many?", "source": {"tpch": 0.01}})
    server.wait_all()
    parent = out.messages[-1]["result"]["run_id"]
    server.handle({"id": "r2", "type": "run", "prompt": "And?", "parent_run_id": "nope"})
    assert out.messages[-1] == {"id": "r2", "type": "error",
                                "error": "no finished run 'nope' in the traces to follow up on"}
    server.handle({"id": "r3", "type": "run", "prompt": "And?", "parent_run_id": parent,
                   "source": {"load": [str(tmp_path / "a.csv")]}})
    (tmp_path / "a.csv").write_text("x\n1\n")
    server.wait_all()
    assert out.messages[-1]["type"] == "error" and "not to loaded files" in out.messages[-1]["error"]
    server.handle({"id": "r4", "type": "run", "prompt": "And?", "parent_run_id": 7})
    assert out.messages[-1]["type"] == "error" and "parent_run_id" in out.messages[-1]["error"]


def test_serve_follow_up_needs_traces():
    out = Output()
    Server(None, out).handle({"id": "r", "type": "run", "prompt": "And?", "parent_run_id": "x"})
    assert out.messages[-1]["type"] == "error" and "traces are off" in out.messages[-1]["error"]


# --- the CLI ---

def test_cli_follow_up_runs_on_the_parents_data(tmp_path, capsys):
    db = str(tmp_path / "shop.duckdb")
    seed = duckdb.connect(db)
    seed.execute("CREATE SCHEMA sales")
    seed.execute("CREATE TABLE sales.orders AS SELECT range AS id FROM range(3)")
    seed.close()
    traces = str(tmp_path / "traces.duckdb")
    common = ["--traces-db", traces, "--no-traces-server", "--quiet"]
    first = FakeClient(response("tool_use", tool_use("SELECT count(*) FROM orders")), response("end_turn", text("3 orders.")))
    assert agent_harness.main(["--db", db, "--schema", "sales", "--prompt", "How many?", *common], client=first) == 0
    parent = duckdb.connect(traces).execute("SELECT run_id FROM agent_runs").fetchone()[0]

    second = FakeClient(response("tool_use", tool_use("SELECT max(id) FROM orders")), response("end_turn", text("Max id 2.")))
    assert agent_harness.main(["--follow-up", parent, "--prompt", "Highest id?", *common], client=second) == 0
    assert "How many?" in first_message_of(second, 0)
    assert "is_error" not in second.sent_messages[1][-1]["content"][0]  # ran on shop.duckdb's sales schema
    rows = duckdb.connect(traces).execute("SELECT parent_run_id, source_db FROM agent_runs ORDER BY started_at").fetchall()
    assert rows[1] == (parent, db)

    assert agent_harness.main(["--follow-up", "nope", "--prompt", "And?", *common], client=FakeClient()) == 1
    assert "no finished run 'nope'" in capsys.readouterr().err


def test_cli_follow_up_needs_a_prompt():
    with pytest.raises(SystemExit):
        agent_harness.parse_args(["--follow-up", "r1"])
    with pytest.raises(SystemExit):
        agent_harness.parse_args(["--follow-up", "r1", "--prompt", "And?", "--reset-traces"])
