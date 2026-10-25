"""The traces database: schema, connection, and writers for runs, API calls, tool calls, and events.

A TracesDB writes either straight to the DuckDB file, or through a Quack server that
owns the file (see traces_server), so other processes can read the traces while a run
writes them. Reads look the same either way: traces_db.execute("SELECT ... FROM agent_runs").
"""

import duckdb
import hashlib
import os
from datetime import datetime

from .serialization import json_dumps
from .usage import USAGE_FIELDS


# Every table's columns, in order. One definition drives the DDL, the insert casts
# (VARIANT columns are bound as JSON text and cast), and the check that an existing
# file has this schema.
#
# Rows are only ever inserted, never updated: Quack can't forward UPDATE or column
# defaults that call a sequence, so ids come from TracesDB.next_id, not DEFAULT nextval.
TRACES_TABLES = {
    # One row per run, written when it ends. A run in progress has a 'run_start' row
    # in agent_traces and no row here yet.
    "agent_runs": {
        "run_id": "VARCHAR PRIMARY KEY",
        "timestamp": "TIMESTAMP",  # equals ended_at; kept for older queries
        "initial_sql": "VARCHAR",
        "table_name": "VARCHAR",
        "prompt": "VARCHAR",
        "final_response": "VARCHAR",
        "total_iterations": "INTEGER",
        "status": "VARCHAR",
        "stop_reason": "VARCHAR",
        "error": "VARCHAR",
        # The run's settings: model on its own for easy filtering, the full AgentConfig
        # (system prompt as a hash), so runs can be compared by configuration: config.effort.
        "model": "VARCHAR",
        "config": "VARIANT",
        # The database the agent's SQL ran against (see describe_source).
        "source_db": "VARCHAR",
        "source_catalog": "VARCHAR",
        "source_schema": "VARCHAR",
        # The tables the answer rests on (schema.table): from submit_answer's
        # supporting_sql in structured runs ('supporting_sql'), else from the last
        # successful query ('last_query'). NULL when the run didn't produce an answer.
        "answer_tables": "VARCHAR[]",
        "answer_tables_from": "VARCHAR",
        # Run timing and totals across all API calls.
        "started_at": "TIMESTAMP",
        "ended_at": "TIMESTAMP",
        "duration_ms": "BIGINT",
        "api_calls": "INTEGER",
        "input_tokens": "BIGINT",
        "cache_creation_input_tokens": "BIGINT",
        "cache_read_input_tokens": "BIGINT",
        "output_tokens": "BIGINT",
        "estimated_cost_usd": "DOUBLE",
    },
    # Everything that happened in a run, in order: run_start, schema_lookup, tool_call,
    # thinking, context_edit, claude_response, submit_answer, api_error, ...
    "agent_traces": {
        "id": "BIGINT PRIMARY KEY",
        "run_id": "VARCHAR",
        "iteration": "INTEGER",
        "timestamp": "TIMESTAMP",
        "action": "VARCHAR",
        "sql_query": "VARCHAR",  # query_database only: the SQL exactly as Claude sent it
        "tool_input": "VARIANT",  # every tool's input: tool_input.sql, tool_input.table, ...
        "query_result": "VARIANT",  # the tool result: query_result.rows, query_result.error, ...
        "claude_response": "VARCHAR",  # text only
        "details": "VARIANT",  # structured events: run_start, context_edit, submit_answer
        "tool_calls": "VARCHAR",
        "tool_name": "VARCHAR",
        # Claude's summarized reasoning, on rows with action = 'thinking' (one per response).
        "thinking": "VARCHAR",
    },
    # One row per API call (successful or failed), for per-iteration token, latency,
    # and cost breakdowns. request_id is what Anthropic support needs to look up a call.
    "api_calls": {
        "id": "BIGINT PRIMARY KEY",
        "run_id": "VARCHAR",
        "iteration": "INTEGER",
        "started_at": "TIMESTAMP",
        "latency_ms": "BIGINT",
        "model": "VARCHAR",
        "request_id": "VARCHAR",
        "stop_reason": "VARCHAR",
        "input_tokens": "BIGINT",
        "cache_creation_input_tokens": "BIGINT",
        "cache_read_input_tokens": "BIGINT",
        "output_tokens": "BIGINT",
        "estimated_cost_usd": "DOUBLE",
        "error": "VARCHAR",
    },
    # One row per tool call: timing, outcome, and how big the result sent to Claude was
    # (result_chars -- every result is resent on each later turn, so this is what drives
    # context growth). error_type is a stable class; error is the full message.
    "tool_calls": {
        "id": "BIGINT PRIMARY KEY",
        "run_id": "VARCHAR",
        "iteration": "INTEGER",
        "tool_name": "VARCHAR",
        "tool_call_id": "VARCHAR",
        "input_sql": "VARCHAR",  # query_database only; NULL for the other tools
        "tool_input": "VARIANT",
        # The tables the call looked at, as schema.table: those list_tables returned, the
        # one describe_table/sample_rows was given, or those a query reads (parsed, not
        # run; NULL if it didn't parse). See the table_choices view.
        "table_names": "VARCHAR[]",
        "output_rows": "INTEGER",
        "output_summary": "VARCHAR",
        "started_at": "TIMESTAMP",
        "duration_ms": "BIGINT",
        "success": "BOOLEAN",
        "truncated": "BOOLEAN",
        "result_chars": "INTEGER",
        "error_type": "VARCHAR",
        "error": "VARCHAR",
    },
}


# The id sequence of each table that has an id column.
ID_SEQUENCES = {"agent_traces": "seq_traces", "api_calls": "seq_api_calls", "tool_calls": "seq_tool_calls"}


# Each query Claude ran as a script you can paste into the duckdb CLI: attach the
# source read-only, USE the same schema (Claude's SQL relies on it for unqualified
# names), then the SQL, ending in a semicolon. A semicolon after a trailing -- comment
# would be commented out, so it goes on its own line there. The setup comes from the
# run_start event, so it's there while the run is still going.
REPLAY_SQL_VIEW = """
    CREATE OR REPLACE VIEW replay_sql AS
    WITH sources AS (
        SELECT run_id,
               details.source_db::VARCHAR AS source_db,
               details.source_catalog::VARCHAR AS source_catalog,
               details.source_schema::VARCHAR AS source_schema
        FROM agent_traces WHERE action = 'run_start'
    ), queries AS (
        SELECT id, run_id, iteration, started_at, success, error_type,
               rtrim(trim(input_sql), '; ' || chr(10) || chr(9)) AS query
        FROM tool_calls WHERE input_sql IS NOT NULL
    )
    SELECT q.id, q.run_id, q.iteration, q.started_at, q.success, q.error_type, q.query,
           concat_ws(chr(10),
               CASE
                   WHEN s.source_db IS NOT NULL
                   THEN 'ATTACH ''' || replace(s.source_db, '''', '''''') || ''' AS "'
                        || s.source_catalog || '" (READ_ONLY);'
                   WHEN s.source_catalog IS NOT NULL
                   THEN '-- The source was an in-memory database: load the same data first.'
               END,
               CASE WHEN s.source_catalog IS NOT NULL
                    THEN 'USE "' || s.source_catalog || '"."' || s.source_schema || '";' END,
               q.query || CASE WHEN contains(regexp_extract(q.query, '[^\\n]*$'), '--')
                               THEN chr(10) || ';' ELSE ';' END
           ) AS script
    FROM queries q LEFT JOIN sources s USING (run_id)
"""


# One row per run and table it came across: how the agent looked at it, whether it
# queried it, and whether the answer used it. Wrong answers often come from the wrong
# table rather than wrong SQL, and this is where that shows:
#     SELECT * FROM table_choices WHERE dropped           -- checked, then not used
# `checked` means preloaded, described, or sampled; listing alone isn't checking.
# `dropped`: checked or queried (even if the query failed), but not among the answer's
# tables (once the run has an answer), or never successfully queried (while it runs,
# or when it ended without an answer).
TABLE_CHOICES_VIEW = """
    CREATE OR REPLACE VIEW table_choices AS
    WITH touches AS (
        SELECT run_id, iteration, 'preloaded' AS how,
               coalesce(query_result.table::VARCHAR, lower(tool_input.table::VARCHAR)) AS table_name
        FROM agent_traces WHERE action = 'schema_lookup'
        UNION ALL
        SELECT run_id, iteration,
               CASE tool_name  -- tool_calls holds the TOOL_LABELS names
                   WHEN 'List Tables' THEN 'listed'
                   WHEN 'Describe Table' THEN 'described'
                   WHEN 'Sample Rows' THEN 'sampled'
                   ELSE CASE WHEN success THEN 'queried' ELSE 'failed_query' END
               END,
               unnest(table_names)
        FROM tool_calls WHERE table_names IS NOT NULL
    ), per_table AS (
        SELECT run_id, table_name,
               bool_or(how = 'listed') AS listed,
               bool_or(how = 'preloaded') AS preloaded,
               bool_or(how = 'described') AS described,
               bool_or(how = 'sampled') AS sampled,
               count(*) FILTER (WHERE how = 'queried') AS queries,
               count(*) FILTER (WHERE how = 'failed_query') AS failed_queries,
               min(iteration) FILTER (WHERE how <> 'listed') AS first_used_iteration
        FROM touches GROUP BY run_id, table_name
    )
    SELECT t.run_id, t.table_name, t.listed, t.preloaded, t.described, t.sampled,
           t.preloaded OR t.described OR t.sampled AS checked,
           t.queries, t.failed_queries, t.first_used_iteration,
           list_contains(r.answer_tables, t.table_name) AS in_answer,
           r.answer_tables_from,
           (t.preloaded OR t.described OR t.sampled OR t.queries > 0 OR t.failed_queries > 0)
               AND NOT coalesce(list_contains(r.answer_tables, t.table_name), t.queries > 0) AS dropped
    FROM per_table t LEFT JOIN agent_runs r USING (run_id)
"""


# Changes whenever the tables or views do. A traces server records the version it set
# up, so a client running newer code can tell the server is out of date and restart it.
SCHEMA_VERSION = hashlib.sha256(
    (repr(TRACES_TABLES) + REPLAY_SQL_VIEW + TABLE_CHOICES_VIEW).encode()
).hexdigest()[:12]


class TracesSchemaError(Exception):
    """The traces database exists but has a different schema (from an older version)."""


def create_traces_schema(conn: duckdb.DuckDBPyConnection) -> None:
    """Create the traces tables, sequences, and views in `conn` (a direct connection to
    the file). Columns added since a file was made are added to it; a file whose
    columns have different types (from before DuckDB 2.0) is refused."""
    existing = {}
    for table, column, data_type in conn.execute("""
        SELECT table_name, column_name, data_type FROM information_schema.columns
        WHERE table_catalog = current_database() AND table_schema = 'main'
    """).fetchall():
        existing.setdefault(table, {})[column] = data_type
    for table, columns in TRACES_TABLES.items():
        if table not in existing:
            continue
        changed = [
            name for name, spec in columns.items()
            if name in existing[table] and existing[table][name] != spec.split()[0]
        ]
        if changed:
            raise TracesSchemaError(
                f"{table} in the traces database has an older schema. Start the traces over "
                "with --reset-traces (or reset_traces_db), or point --traces-db at a new file."
            )

    for sequence in ID_SEQUENCES.values():
        conn.execute(f"CREATE SEQUENCE IF NOT EXISTS {sequence} START 1")
    for table, columns in TRACES_TABLES.items():
        column_ddl = ",\n".join(f"    {name} {spec}" for name, spec in columns.items())
        conn.execute(f"CREATE TABLE IF NOT EXISTS {table} (\n{column_ddl}\n)")
        for name, spec in columns.items():
            if table in existing and name not in existing[table]:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {spec.split()[0]}")
    conn.execute(REPLAY_SQL_VIEW)
    conn.execute(TABLE_CHOICES_VIEW)


class TracesDB:
    """A connection to the traces database. Write with insert(); read with execute().

    Direct (`quack_uri` None): `conn` has the file open, and is the only process that can.
    Remote: `conn` is an in-memory database with the Quack server's database attached
    as `traces` and in USE, so the same unqualified SQL reads the same tables. Quack
    (DuckDB 2.0.0) doesn't forward query parameters, so remote reads that pass `params`
    fail: put literals in the SQL instead. insert() works around this for writes.
    """

    def __init__(self, conn: duckdb.DuckDBPyConnection, quack_uri: str = None, token: str = None):
        self.conn = conn
        self.quack_uri = quack_uri
        self._token = token
        self._staged = set()

    @property
    def remote(self) -> bool:
        return self.quack_uri is not None

    def execute(self, sql: str, params=None):
        return self.conn.execute(sql, params) if params is not None else self.conn.execute(sql)

    def close(self) -> None:
        self.conn.close()

    def next_id(self, table: str) -> int:
        sequence = ID_SEQUENCES[table]
        if not self.remote:
            return self.conn.execute(f"SELECT nextval('{sequence}')").fetchone()[0]
        # quack_query runs the statement on the server, where the sequence lives.
        return self.conn.execute(
            "SELECT * FROM quack_query(?, ?, token = ?)",
            [self.quack_uri, f"SELECT nextval('{sequence}')", self._token]
        ).fetchone()[0]

    def insert(self, table: str, row: dict) -> None:
        """Insert one row. VARIANT columns take Python values (dicts, lists, ...), stored
        as JSON-shaped VARIANTs; tables with an id column get the next id."""
        columns = TRACES_TABLES[table]
        if table in ID_SEQUENCES:
            row = {"id": self.next_id(table), **row}
        names = ", ".join(row)
        placeholders = ", ".join(
            "?::JSON::VARIANT" if columns[name] == "VARIANT" else "?" for name in row
        )
        values = [
            json_dumps(value) if columns[name] == "VARIANT" and value is not None else value
            for name, value in row.items()
        ]
        if not self.remote:
            self.conn.execute(f"INSERT INTO {table} ({names}) VALUES ({placeholders})", values)
            return
        # Quack doesn't forward prepared-statement parameters, so the row is bound into
        # a local temp table and copied across in one statement.
        stage = f"temp.main.stage_{table}"
        if table not in self._staged:
            self.conn.execute(f"CREATE TEMP TABLE IF NOT EXISTS stage_{table} AS FROM {table} LIMIT 0")
            self._staged.add(table)
        try:
            self.conn.execute(f"INSERT INTO {stage} ({names}) VALUES ({placeholders})", values)
            self.conn.execute(f"INSERT INTO {table} ({names}) SELECT {names} FROM {stage}")
        finally:
            self.conn.execute(f"DELETE FROM {stage}")


def reset_traces_db(traces_db_path: str = "agent_traces.duckdb") -> bool:
    """Delete the traces database (and its write-ahead log) so the next connection starts
    from an empty schema, stopping its Quack server first if one is running. Returns
    True if anything was deleted."""
    # Imported here because traces_server imports this module.
    from .traces_server import stop_traces_server

    stop_traces_server(traces_db_path)
    deleted = False
    for path in (traces_db_path, traces_db_path + ".wal"):
        if os.path.exists(path):
            os.remove(path)
            deleted = True
    return deleted


def init_traces_db(traces_db_path: str = "agent_traces.duckdb") -> TracesDB:
    """Open the traces file directly (this process holds it) and create the schema.
    To share it with readers in other processes, use connect_traces_db instead."""
    conn = duckdb.connect(traces_db_path)
    try:
        create_traces_schema(conn)
    except BaseException:
        conn.close()
        raise
    return TracesDB(conn)


def describe_source(conn: duckdb.DuckDBPyConnection) -> dict:
    """Where the agent's SQL runs: the current database's path as you'd ATTACH it
    (None when in memory), its catalog name, and the current schema."""
    try:
        path, db_type, catalog, schema = conn.execute("""
            SELECT path, type, current_database(), current_schema()
            FROM duckdb_databases() WHERE database_name = current_database()
        """).fetchone()
    except (duckdb.Error, TypeError):
        return {"source_db": None, "source_catalog": None, "source_schema": None}
    if path and db_type != "duckdb":
        path = f"{db_type}:{path}"  # e.g. ducklake:metadata.ducklake
    return {"source_db": path, "source_catalog": catalog, "source_schema": schema}


def log_trace(
    traces_conn: TracesDB,
    run_id: str,
    iteration: int,
    action: str,
    sql_query: str = None,
    tool_input: dict = None,
    query_result: dict = None,
    claude_response: str = None,
    details: dict = None,
    tool_calls: str = None,
    tool_name: str = None,
    thinking: str = None
):
    """Log an agent trace. tool_input, query_result, and details are stored as VARIANTs."""
    traces_conn.insert("agent_traces", {
        "run_id": run_id,
        "iteration": iteration,
        "timestamp": datetime.now(),
        "action": action,
        "sql_query": sql_query,
        "tool_input": tool_input,
        "query_result": query_result,
        "claude_response": claude_response,
        "details": details,
        "tool_calls": tool_calls,
        "tool_name": tool_name,
        "thinking": thinking,
    })


def log_tool_call(
    traces_conn: TracesDB,
    run_id: str,
    iteration: int,
    tool_name: str,
    tool_call_id: str,
    input_sql: str = None,
    tool_input: dict = None,
    table_names: list[str] = None,
    output_rows: int = None,
    output_summary: str = None,
    started_at: datetime = None,
    duration_ms: int = None,
    result: dict = None,
    result_chars: int = None
):
    """Log a tool call execution. `result` is the tool's result dict; success,
    truncation, and error details are taken from it."""
    result = result or {}
    traces_conn.insert("tool_calls", {
        "run_id": run_id,
        "iteration": iteration,
        "tool_name": tool_name,
        "tool_call_id": tool_call_id,
        "input_sql": input_sql,
        "tool_input": tool_input,
        "table_names": table_names,
        "output_rows": output_rows,
        "output_summary": output_summary,
        "started_at": started_at,
        "duration_ms": duration_ms,
        "success": result.get("success"),
        "truncated": bool(result.get("truncated")) if result else None,
        "result_chars": result_chars,
        "error_type": result.get("error_type"),
        "error": result.get("error"),
    })


def log_run_summary(
    traces_conn: TracesDB,
    run_id: str,
    table_name: str,
    prompt: str,
    final_response: str,
    total_iterations: int,
    status: str = None,
    stop_reason: str = None,
    error: str = None,
    model: str = None,
    config: dict = None,
    source: dict = None,
    answer_tables: list[str] = None,
    answer_tables_from: str = None,
    started_at: datetime = None,
    duration_ms: int = None,
    api_calls: int = None,
    usage: dict = None,
    estimated_cost_usd: float = None
):
    """Log the summary of an agent run. `config` is AgentConfig.to_dict(); `source` is
    describe_source(); `usage` is AgentResult.usage (token totals by USAGE_FIELDS name)."""
    ended_at = datetime.now()
    usage = usage or {}
    source = source or {}
    traces_conn.insert("agent_runs", {
        "run_id": run_id,
        "timestamp": ended_at,
        "table_name": table_name,
        "prompt": prompt,
        "final_response": final_response,
        "total_iterations": total_iterations,
        "status": status,
        "stop_reason": stop_reason,
        "error": error,
        "model": model,
        "config": config,
        "source_db": source.get("source_db"),
        "source_catalog": source.get("source_catalog"),
        "source_schema": source.get("source_schema"),
        "answer_tables": answer_tables,
        "answer_tables_from": answer_tables_from,
        "started_at": started_at,
        "ended_at": ended_at,
        "duration_ms": duration_ms,
        "api_calls": api_calls,
        "input_tokens": usage.get("input_tokens"),
        "cache_creation_input_tokens": usage.get("cache_creation_input_tokens"),
        "cache_read_input_tokens": usage.get("cache_read_input_tokens"),
        "output_tokens": usage.get("output_tokens"),
        "estimated_cost_usd": estimated_cost_usd,
    })


def log_api_call(
    traces_conn: TracesDB,
    run_id: str,
    iteration: int,
    started_at: datetime,
    latency_ms: int,
    model: str,
    request_id: str = None,
    stop_reason: str = None,
    usage=None,
    estimated_cost_usd: float = None,
    error: str = None
):
    """Log one API call. `usage` is the response's usage object (None for failed calls)."""
    row = {
        "run_id": run_id,
        "iteration": iteration,
        "started_at": started_at,
        "latency_ms": latency_ms,
        "model": model,
        "request_id": request_id,
        "stop_reason": stop_reason,
    }
    for name in USAGE_FIELDS:
        # Unused cache fields come back as None; store 0 so sums work.
        row[name] = (getattr(usage, name, None) or 0) if usage is not None else None
    row["estimated_cost_usd"] = estimated_cost_usd
    row["error"] = error
    traces_conn.insert("api_calls", row)
