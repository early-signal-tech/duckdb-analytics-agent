"""Takes: what a rack's chain played, read back from the traces.

A take is one press of PLAY: a single run, an A/B pair (both runs share labels.pair), or a
setlist (its runs share labels.setlist_run). Runs played from a rack record the rack preset
in labels.rack_preset, so a preset's takes are its runs grouped by take. Nothing about
takes is stored: they're worked out from the runs' labels.

    list_takes(traces, "rack-…")  -> newest first: {key, kind, started_at, ended_at, runs,
                                     completed, cost_usd, prompt, setlist_name, question_set}
    take_runs(traces, key)        -> the take's runs, in order: {run_id, prompt, answer,
                                     status, error, iterations, duration_ms, cost_usd, labels}

Runs are written to the traces when they finish, so a take that's still playing shows
the runs done so far.
"""

import json

from .tracing import TracesDB

TAKE_KEY = "coalesce(labels.setlist_run::VARCHAR, labels.pair::VARCHAR, run_id)"
TAKE_KIND = (
    "CASE WHEN labels.setlist_run IS NOT NULL THEN 'setlist' "
    "WHEN labels.pair IS NOT NULL THEN 'ab' ELSE 'run' END"
)


def list_takes(traces: TracesDB, rack_preset: str | None, limit: int = 50) -> list[dict]:
    """A rack preset's takes, newest first. With no preset: the takes played outside any
    rack preset (the standalone apps, the CLI, the rack before presets)."""
    where = "labels.rack_preset::VARCHAR = ?" if rack_preset else "labels.rack_preset IS NULL"
    params = [rack_preset] if rack_preset else []
    rows = traces.execute(f"""
        SELECT {TAKE_KEY} AS key,
               any_value({TAKE_KIND}) AS kind,
               min(started_at) AS started_at,
               max(ended_at) AS ended_at,
               count(*) AS runs,
               count(*) FILTER (WHERE status = 'completed') AS completed,
               sum(estimated_cost_usd) AS cost_usd,
               any_value(prompt) AS prompt,
               any_value(labels.setlist_name::VARCHAR) AS setlist_name,
               any_value(labels.setlist::VARCHAR) AS question_set
        FROM agent_runs
        WHERE {where}
        GROUP BY 1
        ORDER BY min(started_at) DESC
        LIMIT {int(limit)}
    """, params).fetchall()
    keys = ["key", "kind", "started_at", "ended_at", "runs", "completed", "cost_usd", "prompt", "setlist_name", "question_set"]
    out = []
    for row in rows:
        take = dict(zip(keys, row))
        for k in ("started_at", "ended_at"):
            take[k] = take[k].isoformat() if take[k] is not None else None
        if take["kind"] == "setlist":
            take["prompt"] = None  # a setlist has many prompts; its name and set say what it was
        out.append(take)
    return out


def take_runs(traces: TracesDB, key: str) -> list[dict]:
    """One take's runs: the run itself, A then B, or a setlist's runs by question, column,
    and repeat."""
    rows = traces.execute(f"""
        SELECT run_id, prompt, final_response, status, error, total_iterations, duration_ms,
               estimated_cost_usd, labels::JSON::VARCHAR
        FROM agent_runs
        WHERE {TAKE_KEY} = ?
        ORDER BY labels.ab::VARCHAR NULLS FIRST, labels.question::VARCHAR, labels."column"::VARCHAR,
                 labels."repeat"::INTEGER, started_at
    """, [key]).fetchall()
    keys = ["run_id", "prompt", "answer", "status", "error", "iterations", "duration_ms", "cost_usd", "labels"]
    out = []
    for row in rows:
        run = dict(zip(keys, row))
        run["labels"] = json.loads(run["labels"]) if run["labels"] else {}
        out.append(run)
    return out
