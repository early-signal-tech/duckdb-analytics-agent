"""Conversations: a question and its follow-ups, read back from the traces.

A conversation is a chain of runs linked by parent_run_id (see follow_up.py), starting at
a run with no parent. Like takes, nothing about conversations is stored: they're worked
out from agent_runs. A chat (CH-1 or CHAT in the rack) lists a rack preset's
conversations and reopens one to show its turns or ask a follow-up.

Each chat tags its runs with labels.chat ("ch1", "chat"), so each lists only its own
conversations: CH-1 doesn't list CHAT's, and neither lists runs from DH-1 or the command
line. CH-1's runs from before the tag (2026-11-28) have none; inside a rack preset, an
untagged conversation is CH-1's, since CH-1 was the only chat then.

    list_conversations(traces, "rack-…", chat="ch1")  -> newest activity first: {root_run_id,
        last_run_id, turns, title, started_at, ended_at, cost_usd, last_status, data_source}
    conversation_turns(traces, run_id)    -> the conversation up to run_id, oldest first:
        {run_id, parent_run_id, prompt, answer, status, error, iterations, queries,
         duration_ms, cost_usd, started_at, data_source}

A rack preset's conversations start at its runs that aren't part of an A/B pair or a
setlist. Runs are written when they finish, so a turn still running isn't listed yet.
If a run was followed up more than once, its conversation's last run is the newest.

Values go into the SQL as quoted literals, not as parameters: through the traces server
(Quack), prepared-statement parameters aren't forwarded.
"""

import json

from .follow_up import MAX_TURNS
from .tracing import TracesDB, sql_literal as _lit

__all__ = ["MAX_TURNS", "conversation_turns", "list_conversations"]


def _json(value):
    return json.loads(value) if value is not None else None


def _iso(value):
    return value.isoformat() if value is not None else None


# The chat that asked conversations before chats tagged their runs.
UNTAGGED_CHAT = "ch1"


def list_conversations(traces: TracesDB, rack_preset: str | None, limit: int = 50, chat: str | None = None) -> list[dict]:
    """A rack preset's conversations, the most recently active first. With no preset: those
    started outside any rack preset. With `chat`: only that chat's (labels.chat), plus,
    for CH-1 in a rack preset, the untagged ones from before the tag."""
    preset = f"labels.rack_preset::VARCHAR = {_lit(rack_preset)}" if rack_preset else "labels.rack_preset IS NULL"
    if chat is not None:
        tagged = f"labels.chat::VARCHAR = {_lit(chat)}"
        preset += f" AND ({tagged} OR labels.chat IS NULL)" if chat == UNTAGGED_CHAT and rack_preset else f" AND {tagged}"
    rows = traces.execute(f"""
        WITH RECURSIVE chain(run_id, root) AS (
            SELECT run_id, run_id FROM agent_runs
            WHERE parent_run_id IS NULL AND {preset}
              AND labels.pair IS NULL AND labels.setlist_run IS NULL
            UNION ALL
            SELECT r.run_id, c.root FROM agent_runs r JOIN chain c ON r.parent_run_id = c.run_id
        )
        SELECT c.root,
               arg_max(r.run_id, r.ended_at) AS last_run_id,
               count(*) AS turns,
               any_value(r.prompt) FILTER (WHERE r.run_id = c.root) AS title,
               min(r.started_at) AS started_at,
               max(r.ended_at) AS ended_at,
               sum(r.estimated_cost_usd) AS cost_usd,
               arg_max(r.status, r.ended_at) AS last_status,
               arg_max(r.data_source::JSON::VARCHAR, r.ended_at) AS data_source
        FROM chain c JOIN agent_runs r USING (run_id)
        GROUP BY c.root
        ORDER BY max(r.ended_at) DESC
        LIMIT {int(limit)}
    """).fetchall()
    keys = ["root_run_id", "last_run_id", "turns", "title", "started_at", "ended_at", "cost_usd", "last_status", "data_source"]
    out = []
    for row in rows:
        c = dict(zip(keys, row))
        c["started_at"], c["ended_at"] = _iso(c["started_at"]), _iso(c["ended_at"])
        c["data_source"] = _json(c["data_source"])
        out.append(c)
    return out


def conversation_turns(traces: TracesDB, run_id: str) -> list[dict]:
    """The conversation up to and including `run_id`, oldest first. Empty if the run isn't
    in the traces (unknown, or still running). An earlier turn that went missing (traces
    trimmed) ends the walk: what's there is returned."""
    chain, at, seen = [], run_id, set()
    while at is not None and at not in seen:
        seen.add(at)
        row = traces.execute(f"SELECT parent_run_id FROM agent_runs WHERE run_id = {_lit(at)}").fetchone()
        if row is None:
            break
        chain.append(at)
        at = row[0]
    if not chain:
        return []
    ids = ", ".join(_lit(r) for r in chain)
    rows = traces.execute(f"""
        SELECT r.run_id, r.parent_run_id, r.prompt, r.final_response, r.status, r.error,
               r.total_iterations,
               (SELECT count(*) FROM tool_calls t WHERE t.run_id = r.run_id AND t.input_sql IS NOT NULL),
               r.duration_ms, r.estimated_cost_usd, r.started_at, r.data_source::JSON::VARCHAR
        FROM agent_runs r WHERE r.run_id IN ({ids})
    """).fetchall()
    keys = ["run_id", "parent_run_id", "prompt", "answer", "status", "error", "iterations", "queries",
            "duration_ms", "cost_usd", "started_at", "data_source"]
    by_id = {}
    for row in rows:
        turn = dict(zip(keys, row))
        turn["started_at"] = _iso(turn["started_at"])
        turn["data_source"] = _json(turn["data_source"])
        by_id[turn["run_id"]] = turn
    return [by_id[r] for r in reversed(chain) if r in by_id]
