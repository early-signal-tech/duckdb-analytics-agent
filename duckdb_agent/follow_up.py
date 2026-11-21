"""Follow-up questions: a run that continues an earlier one, with context read from the traces.

A follow-up names its parent run (run_agent(parent_run_id=...)). The earlier turns are
put in the first message as <previous_turn> blocks, a digest of each: the question, the
answer, the SQL the answer rests on, and its tables. Nothing is replayed: Claude re-runs
or adapts the earlier SQL when it needs those numbers again.

Only facts are stored (agent_runs.parent_run_id, and a follow_up_context event naming
the turns that went in); the digest is rebuilt from the traces each time.

    thread = load_thread(traces, parent_run_id)      # earlier turns, oldest first
    follow_up = build_follow_up(traces, parent_run_id, data_source)
    follow_up.text                                   # the <previous_turn> blocks

Rules, checked before the run starts (FollowUpError otherwise):
  - A conversation has at most MAX_TURNS questions, counting the new one.
  - The parent must have finished (it has an agent_runs row).
  - A follow-up can stay on the parent's data or switch to another database (a db file,
    a ducklake: URI, remote data), but not to loaded files or TPC-H data.
"""

import json
from dataclasses import dataclass

from .tracing import TracesDB, sql_literal


# Questions in one conversation, counting the new one. Analytical follow-ups rarely go
# deeper, and keeping it short means every earlier turn fits whole.
MAX_TURNS = 3

# An earlier answer longer than this is cut, with a note.
MAX_ANSWER_CHARS = 6_000


class FollowUpError(ValueError):
    """A follow-up that can't run: unknown parent, too many turns, or a disallowed source."""


@dataclass
class FollowUp:
    parent_run_id: str
    turns: list[dict]  # oldest first; see load_thread
    data_source: dict | None  # the new run's
    text: str  # the <previous_turn> blocks for the first message

    @property
    def source_changed(self) -> bool:
        return self.data_source != self.turns[-1]["data_source"]

    def details(self) -> dict:
        """What went into the digest, for the follow_up_context trace event."""
        return {
            "parent_run_id": self.parent_run_id,
            "turns": [
                {"run_id": t["run_id"], "status": t["status"], "sql_from": t["sql_from"],
                 "answer_cut": len(t["answer"] or "") > MAX_ANSWER_CHARS}
                for t in self.turns
            ],
            "source_changed": self.source_changed,
        }


def _json(value):
    return json.loads(value) if value is not None else None


def load_turn(traces: TracesDB, run_id: str) -> dict | None:
    """One finished run as a turn: run_id, parent_run_id, prompt, answer, status,
    data_source, answer_tables, sql (list), sql_from ('supporting_sql', 'last_query',
    or None). None if the run isn't in agent_runs (unknown, or still running)."""
    # Values are inlined: the traces server (Quack) doesn't forward query parameters.
    row = traces.execute(f"""
        SELECT run_id, parent_run_id, prompt, final_response, status,
               data_source::JSON::VARCHAR, answer_tables
        FROM agent_runs WHERE run_id = {sql_literal(run_id)}
    """).fetchone()
    if row is None:
        return None
    turn = dict(zip(["run_id", "parent_run_id", "prompt", "answer", "status", "data_source", "answer_tables"], row))
    turn["data_source"] = _json(turn["data_source"])
    submitted = traces.execute(f"""
        SELECT details::JSON::VARCHAR FROM agent_traces
        WHERE run_id = {sql_literal(run_id)} AND action = 'submit_answer'
        ORDER BY id DESC LIMIT 1
    """).fetchone()
    supporting = (_json(submitted[0]) or {}).get("supporting_sql") if submitted else None
    if supporting:
        turn["sql"], turn["sql_from"] = list(supporting), "supporting_sql"
    else:
        last = traces.execute(f"""
            SELECT input_sql FROM tool_calls
            WHERE run_id = {sql_literal(run_id)} AND success AND input_sql IS NOT NULL
            ORDER BY id DESC LIMIT 1
        """).fetchone()
        turn["sql"], turn["sql_from"] = ([last[0]], "last_query") if last else ([], None)
    return turn


def load_thread(traces: TracesDB, parent_run_id: str) -> list[dict]:
    """The conversation up to and including `parent_run_id`, oldest first. Raises
    FollowUpError if the parent (or an earlier turn) isn't a finished run, or the
    conversation is already MAX_TURNS long."""
    turns, run_id = [], parent_run_id
    while run_id is not None:
        if len(turns) >= MAX_TURNS:
            break
        turn = load_turn(traces, run_id)
        if turn is None:
            if run_id == parent_run_id:
                raise FollowUpError(f"no finished run {parent_run_id!r} in the traces to follow up on")
            break  # an earlier turn went missing (traces trimmed): keep what's there
        turns.append(turn)
        run_id = turn["parent_run_id"]
    if len(turns) >= MAX_TURNS:
        raise FollowUpError(
            f"this conversation already has {MAX_TURNS} questions, the most a follow-up can build on; "
            "start a new question"
        )
    return turns[::-1]


def check_source(parent_source: dict | None, data_source: dict | None) -> None:
    """A follow-up stays on the parent's data or switches to another database, never to
    loaded files or TPC-H data."""
    if data_source == parent_source:
        return
    if not data_source or "db" not in data_source:
        raise FollowUpError(
            "a follow-up can switch to another connected database, not to loaded files or TPC-H data; "
            "ask it on the earlier run's data, or start a new question"
        )


def source_from_data_source(data_source: dict | None) -> dict | None:
    """A run's recorded data_source as a request's source ({db, schema} | {load} |
    {tpch}), to open the same data again. None when it wasn't recorded."""
    if not data_source:
        return None
    if "db" in data_source:
        return {"db": data_source["db"], "schema": data_source.get("schema")}
    if "load" in data_source:
        return {"load": list(data_source["load"])}
    if "tpch" in data_source:
        return {"tpch": data_source["tpch"]}
    return None


def parent_source(traces: TracesDB, parent_run_id: str) -> dict:
    """The parent run's data as a request's source, for a follow-up that doesn't name
    its own. Raises FollowUpError if the run is unknown or its data wasn't recorded."""
    turn = load_turn(traces, parent_run_id)
    if turn is None:
        raise FollowUpError(f"no finished run {parent_run_id!r} in the traces to follow up on")
    source = source_from_data_source(turn["data_source"])
    if source is None:
        raise FollowUpError(f"run {parent_run_id!r} didn't record its data; name the data for the follow-up")
    return source


def describe_data(data_source: dict | None) -> str:
    if not data_source:
        return "unrecorded data"
    if "db" in data_source:
        return data_source["db"] + (f" (schema {data_source['schema']})" if data_source.get("schema") else "")
    if "load" in data_source:
        return "loaded files: " + ", ".join(data_source["load"])
    if "tpch" in data_source:
        return f"TPC-H data at scale {data_source['tpch']}"
    return "unrecorded data"


def render_turn(number: int, turn: dict) -> str:
    answer = turn["answer"] or ""
    if len(answer) > MAX_ANSWER_CHARS:
        answer = answer[:MAX_ANSWER_CHARS] + "\n[answer cut here]"
    status = turn["status"] or "unknown"
    parts = [
        f'<previous_turn number="{number}">',
        f"<question>\n{turn['prompt']}\n</question>",
        f'<answer status="{status}">\n{answer or "(no answer)"}\n</answer>',
    ]
    if turn["sql"]:
        label = "the SQL the answer cited" if turn["sql_from"] == "supporting_sql" else "the last query that ran"
        sql = "\n\n".join(q.strip().rstrip(";") + ";" for q in turn["sql"])
        parts.append(f'<answer_sql from="{label}">\n{sql}\n</answer_sql>')
    if turn["answer_tables"]:
        parts.append(f"<answer_tables>{', '.join(turn['answer_tables'])}</answer_tables>")
    parts.append("</previous_turn>")
    return "\n".join(parts)


def render(turns: list[dict], data_source: dict | None) -> str:
    """The digest of the earlier turns, for the start of the first message."""
    blocks = [render_turn(i, turn) for i, turn in enumerate(turns, 1)]
    parent = turns[-1]["data_source"]
    if data_source != parent:
        blocks.append(
            f"<data_change>The previous turn used {describe_data(parent)}; this question uses "
            f"{describe_data(data_source)}. Earlier SQL may not run here as written.</data_change>"
        )
    return "\n\n".join(blocks)


def build_follow_up(traces: TracesDB, parent_run_id: str, data_source: dict | None) -> FollowUp:
    """Load the conversation, check the rules, and render the digest."""
    if not isinstance(parent_run_id, str) or not parent_run_id.strip():
        raise FollowUpError("parent_run_id must be a run id")
    turns = load_thread(traces, parent_run_id)
    check_source(turns[-1]["data_source"], data_source)
    return FollowUp(parent_run_id, turns, data_source, render(turns, data_source))
