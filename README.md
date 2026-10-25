# Duck Harness

A SQL-to-LLM agent: Claude (Haiku 5.5) answers questions about DuckDB or DuckLake data by writing read-only SQL, reading the results, and iterating until it can answer with evidence. Every step is traced to a DuckDB database.

## Quick start

1. **API key.** Copy `.env.example` to `.env` and set `ANTHROPIC_API_KEY`.
2. **Install.** `uv sync`
3. **Run.**
   ```bash
   uv run agent_harness.py                      # demo question on DuckLake prod (gold.events_rich_ml_gold)
   uv run agent_harness.py --prompt "Which event types grew most last week?"
   uv run agent_harness.py --db analytics.duckdb --schema main --table orders --table customers
   uv run agent_harness.py --load sales.csv --load customers.parquet   # in memory, this run only
   uv run agent_harness.py --tpch --prompt "Top 5 customers by revenue?"   # TPC-H sample data, in memory
   uv run agent_harness.py --tpch --save-db tpch.duckdb --no-run   # build a reusable file, no API call
   uv run agent_harness.py --db tpch.duckdb                        # ...and use it from then on
   uv run agent_harness.py --effort high --structured --quiet
   uv run agent_harness.py --reset-traces      # delete the traces database first, starting the traces over
   uv run agent_harness.py --traces-shell      # open the traces in the duckdb CLI, even while a run writes
   uv run agent_harness.py --stop-traces-server
   uv run agent_harness.py --help
   ```
   `--db` takes a persistent DuckDB file (attached read-only) or a `ducklake:` URI. `--load FILE` (CSV, Parquet, or JSON; repeatable) builds an in-memory database with one table per file, named after the file (`Sales 2026.csv` becomes `sales_2026`); `--tpch [SCALE]` generates the TPC-H benchmark tables instead (scale factor 0.1 by default; the DuckDB `tpch` extension downloads on first use), and the two combine. Both are held in memory and gone when the run ends, unless you add `--save-db PATH`: it writes the data to a new DuckDB file instead (refusing to overwrite an existing one), then runs against it; add `--no-run` to just build the file. Reuse it with `--db PATH`. In-memory databases can't be attached read-only, so there the SQL read-only check is the only guard. Files and ducklake URIs are always attached read-only. Without `--table`, the agent finds tables itself. `--quiet` logs progress only and prints the answer; the default shows the full trace (SQL, thinking, results). Exit status is 0 when the run completes, 1 otherwise. Needs DuckDB 2.0 and Python 3.11+.

## Using it from Python

```python
import duckdb
from duckdb_agent import AgentConfig, connect_traces_db, run_agent

conn = duckdb.connect()
conn.execute("ATTACH 'ducklake:ducklake_prod' AS source (READ_ONLY)")  # always READ_ONLY
conn.execute("USE source.gold")

result = run_agent(
    conn,
    "Compare the most recent day against the prior day.",
    tables="events_rich_ml_gold",          # optional: schemas to preload
    config=AgentConfig(effort="medium"),   # model, effort, limits, features
    traces_conn=connect_traces_db("agent_traces.duckdb"),  # or init_traces_db: the file, no server
)
print(result.status, result.estimated_cost_usd)
print(result.text)
```

`result.status` is `completed` on a normal finish; anything else (`max_iterations`, `api_error`, `refusal`, ...) is described on `AgentResult` in `duckdb_agent/loop.py`. With `AgentConfig(structured_answer=True)`, `result.answer` holds `summary`, `key_findings`, `caveats`, and `supporting_sql`.

The library logs through the `duckdb_agent` logger and adds no handlers; call `configure_logging(verbose=True)` for console output in scripts.

## Traces

Every run is recorded in `agent_traces.duckdb`. DuckDB lets only one process open a file for writing, so the traces are written through a background [Quack](https://duckdb.org/docs/2.0/quack/overview) server that owns the file. The first run starts it, on `quack:localhost:9494`, and it keeps running until `--stop-traces-server`, so you can read traces in another terminal while runs write them.

### Connecting to the traces server

From the repo root, open the duckdb CLI connected to the server:

```bash
duckdb -init agent_traces.duckdb.quack.sql
```

or let the harness do it, starting the server first if it isn't running:

```bash
uv run agent_harness.py --traces-shell
```

You get a normal duckdb prompt where `agent_runs`, `agent_traces`, `tool_calls`, `api_calls`, and `replay_sql` work as usual. The init file, written by the server when it starts, attaches the server with its token and runs `CONNECT`, so your queries run on the server. To connect by hand (from another directory, or from Python), use the URI and token from `agent_traces.duckdb.quack.json`:

```sql
ATTACH 'quack:localhost:9494' AS traces (TYPE quack, TOKEN '<TOKEN>');
CONNECT traces;
```

Don't open the file itself while the server runs: `duckdb agent_traces.duckdb` fails with `Could not set lock on file ... Conflicting lock is held in .../python3.13 (PID ...)`, and that PID is the traces server. To work with the file directly, stop the server first (`uv run agent_harness.py --stop-traces-server`); the next run starts it again.

#### A `traces` shortcut

Add an alias once, and `traces` connects from any directory. For zsh (the macOS default), replacing the path with where you cloned the repo:

```bash
echo "alias traces='duckdb -init ~/path/to/duckdb-analytics-agent/agent_traces.duckdb.quack.sql'" >> ~/.zshrc
source ~/.zshrc
```

For bash, use `~/.bashrc` instead. New terminals pick the alias up automatically; `source` loads it into the one you have open. Then:

```bash
traces
```

```sql
SELECT status, total_iterations, left(prompt, 60) AS prompt FROM agent_runs ORDER BY started_at DESC;
.quit
```

If it doesn't work:
- **`command not found: traces`**: the alias isn't loaded in this terminal. Run `source ~/.zshrc` or open a new one.
- **The init file is missing, or the connection is refused**: the server isn't running. The `.quack.sql` file only exists while the server is up, and `--stop-traces-server` deletes it. Start the server again with an agent run or `uv run agent_harness.py --traces-shell`.
- **Wrong path**: `type traces` shows where the alias points.

The server is local-only and needs a token. The token is generated on first start and kept in `agent_traces.duckdb.quack.json` and `.quack.sql`, which only you can read. Its output goes to `agent_traces.duckdb.quack.log`. If the server can't start, runs warn and write the file directly, as does `--no-traces-server`. Replace placeholders like `<RUN ID>` below with real values; Quack in DuckDB 2.0.0 doesn't forward `?` parameters.

Structured fields are `VARIANT`s: query them with dot paths (`config.effort`, `query_result.rows[1]`, `tool_input.table`), cast with `::VARCHAR`/`::INT`, and use `::JSON` to see one as JSON.

```sql
-- Recent runs: outcome, cost, and settings
SELECT run_id, started_at, status, total_iterations, round(estimated_cost_usd, 4) AS cost,
       config.effort::VARCHAR AS effort, source_db, left(prompt, 60) AS prompt
FROM agent_runs ORDER BY started_at DESC LIMIT 10;

-- Runs in progress (agent_runs gets its row when a run ends)
SELECT run_id, timestamp AS started_at, details.prompt::VARCHAR AS prompt
FROM agent_traces s WHERE action = 'run_start'
  AND NOT EXISTS (SELECT 1 FROM agent_runs r WHERE r.run_id = s.run_id);

-- Per-call tokens, caching, and latency for one run
SELECT iteration, latency_ms, input_tokens, cache_read_input_tokens, output_tokens, request_id
FROM api_calls WHERE run_id = '<RUN ID>' ORDER BY id;

-- What Claude ran, and how it went
SELECT iteration, tool_name, success, truncated, duration_ms, error_type,
       left(coalesce(input_sql, tool_input::JSON::VARCHAR), 80) AS input
FROM tool_calls WHERE run_id = '<RUN ID>' ORDER BY id;

-- Claude's reasoning, turn by turn
SELECT iteration, thinking FROM agent_traces WHERE run_id = '<RUN ID>' AND action = 'thinking' ORDER BY id;
```

**Which tables it chose.** Wrong answers often come from the wrong table, not wrong SQL, so every tool call logs the tables it looked at in `tool_calls.table_names` (as `schema.table`): what `list_tables` returned, the table `describe_table` or `sample_rows` was given, or the tables a query reads. Query tables are found by parsing the SQL, without running it again; CTE names are left out, and a query that doesn't parse gets NULL. Each run records `answer_tables`, the tables its answer rests on: from `supporting_sql` in `--structured` runs, otherwise from the last successful query (`answer_tables_from` says which). The `table_choices` view puts it together, one row per run and table:

```sql
-- Tables the agent checked (preloaded, described, or sampled) or queried, then didn't use for the answer
SELECT run_id, table_name, described, sampled, queries, failed_queries, first_used_iteration
FROM table_choices WHERE dropped ORDER BY run_id, first_used_iteration;

-- How often each table gets checked and dropped, across runs
SELECT table_name, count(*) FILTER (WHERE dropped) AS dropped, count(*) FILTER (WHERE in_answer) AS used
FROM table_choices WHERE checked OR queries > 0 GROUP BY ALL ORDER BY dropped DESC;
```

**Re-running Claude's SQL.** `tool_calls.input_sql` holds each `query_database` query exactly as Claude sent it (NULL for the other tools, whose input is in `tool_input`). The `replay_sql` view turns each one into a script you can paste into a fresh `duckdb` session: it attaches the source database read-only under the same name, `USE`s the same schema (Claude's SQL relies on it for unqualified table names), and ends the query with a semicolon. Print it in list mode, because the default box output clips long values:

```sql
.mode list
.headers off
SELECT script FROM replay_sql WHERE run_id = '<RUN ID>' ORDER BY id;
```

For in-memory sources (`--load`, `--tpch` without `--save-db`) the script says so, since that data is gone after the run: rebuild it, or use `--save-db` to make a file you can replay against.

The traces hold SQL, results, and answers, so protect the file like the data itself. A traces file from before DuckDB 2.0 is refused with a pointer to `--reset-traces`.

## Tests

```bash
uv run pytest                                    # no API key needed
uv run pytest --cov=duckdb_agent --cov-branch    # with coverage
```

## Layout

```
├── agent_harness.py       command-line entry point
├── duckdb_agent/          the agent package (traces_server.py: the Quack server)
├── tests/                 pytest suite
├── .env.example           environment template
└── pyproject.toml / uv.lock
```
