"""Run the agent from the command line against a DuckDB file or DuckLake catalog.

    uv run agent_harness.py                                  # the demo question on DuckLake prod
    uv run agent_harness.py --prompt "Which event types grew most last week?"
    uv run agent_harness.py --db analytics.duckdb --schema main --table orders --table customers
    uv run agent_harness.py --load sales.csv --load customers.parquet   # in memory, this run only
    uv run agent_harness.py --tpch                           # TPC-H sample data in memory
    uv run agent_harness.py --tpch --save-db tpch.duckdb --no-run   # build a reusable file
    uv run agent_harness.py --effort high --structured --quiet
    uv run agent_harness.py --traces-shell                   # read the traces in the duckdb CLI

The database is always attached READ_ONLY. Traces are written through a background Quack
server (started on first use, left running) so you can read them while a run writes;
--stop-traces-server stops it. Exit status is 0 when the run completes
normally and 1 otherwise (max iterations, API error, refusal, ...).
"""
import argparse
import os
import re
import shutil
import sys
from pathlib import Path

import duckdb
from dotenv import load_dotenv

from duckdb_agent import AgentConfig, configure_logging, connect_traces_db, reset_traces_db, run_agent, stop_traces_server
from duckdb_agent.traces_server import DEFAULT_PORT, TracesServerError, start_traces_server, state_paths
from duckdb_agent.config import DEFAULT_EFFORT, DEFAULT_MAX_ITERATIONS, DEFAULT_MODEL, EFFORT_LEVELS

IN_MEMORY = ":memory:"

DEFAULT_TPCH_SCALE = 0.1  # ~100 MB of data, generated in a few seconds

DEFAULT_PROMPT = (
    "Compare the most recent day of data against the prior day and run the most important analysis on it."
)


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ask Claude a question about DuckDB data.")
    parser.add_argument("--prompt", default=DEFAULT_PROMPT, help="The question to answer.")
    parser.add_argument(
        "--table", action="append", dest="tables", metavar="TABLE",
        help="A table to focus on; its schema is preloaded. Repeatable. "
             "Default: events_rich_ml_gold when using the default --db; otherwise none "
             "(the agent finds tables with list_tables)."
    )
    parser.add_argument(
        "--no-table", action="store_true",
        help="Preload no schema, even with the default --db: the agent must find tables with list_tables."
    )
    parser.add_argument(
        "--db", default="ducklake:ducklake_prod",
        help="Database to attach read-only: a DuckDB file path or a ducklake: URI. "
             "Default: ducklake:ducklake_prod."
    )
    parser.add_argument(
        "--load", action="append", dest="load", metavar="FILE", default=[],
        help="Load a CSV, Parquet, or JSON file into an in-memory database as a table named after the file. "
             "Repeatable. With --load and no --db, the data lives in memory for this run only. "
             "You can also pass --db :memory: explicitly."
    )
    parser.add_argument(
        "--tpch", nargs="?", type=float, const=DEFAULT_TPCH_SCALE, metavar="SCALE",
        help="Generate the TPC-H benchmark tables (customer, orders, lineitem, ...) in memory. "
             f"Optional scale factor, default {DEFAULT_TPCH_SCALE}. Needs the DuckDB tpch extension, "
             "downloaded on first use. Can be combined with --load."
    )
    parser.add_argument(
        "--save-db", metavar="PATH",
        help="Write the --load / --tpch data to this new DuckDB file instead of memory, then run against it "
             "read-only. Reuse it later with --db PATH. Refuses to overwrite an existing file."
    )
    parser.add_argument(
        "--no-run", action="store_true",
        help="With --save-db: build the database file and exit without calling Claude."
    )
    parser.add_argument("--schema", help="Schema to USE in that database. Default: gold for the default --db.")
    parser.add_argument("--traces-db", default="agent_traces.duckdb", help="Where to write traces.")
    parser.add_argument(
        "--reset-traces", action="store_true",
        help="Delete the traces database before running, so this run starts the traces over."
    )
    parser.add_argument(
        "--no-traces-server", action="store_true",
        help="Write the traces file directly instead of through the background Quack server. "
             "Other processes can't open the file while the run holds it."
    )
    parser.add_argument(
        "--traces-port", type=int, default=DEFAULT_PORT,
        help=f"Port for the traces server, on localhost. Default: {DEFAULT_PORT}."
    )
    parser.add_argument(
        "--traces-shell", action="store_true",
        help="Open the duckdb CLI connected to the traces server (starting it if needed), then exit."
    )
    parser.add_argument(
        "--stop-traces-server", action="store_true",
        help="Stop the background traces server, then exit."
    )
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--effort", default=DEFAULT_EFFORT, choices=EFFORT_LEVELS)
    parser.add_argument("--max-iterations", type=int, default=DEFAULT_MAX_ITERATIONS)
    parser.add_argument("--structured", action="store_true", help="End with a structured answer (submit_answer).")
    parser.add_argument(
        "--quiet", action="store_true",
        help="Log progress and metadata only (no SQL, thinking, or results), then print the answer."
    )
    args = parser.parse_args(argv)
    has_data = bool(args.load) or args.tpch is not None
    if args.no_run and not args.save_db:
        parser.error("--no-run needs --save-db")
    if args.save_db:
        if not has_data:
            parser.error("--save-db needs --load or --tpch to put in the file")
        if args.db != parser.get_default("db"):
            parser.error("--save-db and --db can't be used together; use --db to reuse the file later")
        if Path(args.save_db).exists():
            parser.error(f"{args.save_db} already exists; delete it or pick another path")
        args.db = args.save_db
    elif has_data and args.db == parser.get_default("db"):
        args.db = IN_MEMORY
    if has_data and not args.save_db and args.db != IN_MEMORY:
        parser.error("--load and --tpch need an in-memory database; drop --db or use --db :memory:")
    if args.tpch is not None and args.tpch <= 0:
        parser.error("--tpch scale factor must be positive")
    if args.no_table and args.tables:
        parser.error("--no-table and --table can't be used together")
    if args.db == parser.get_default("db"):
        args.schema = args.schema or "gold"
        if not args.no_table:
            args.tables = args.tables or ["events_rich_ml_gold"]
    return args


def table_name_for(path: str) -> str:
    """A SQL-safe table name from a file name: data/Sales 2026.csv.gz -> sales_2026."""
    stem = Path(path).name.split(".")[0]
    return re.sub(r"\W+", "_", stem).strip("_").lower() or "data"


def populate(conn: duckdb.DuckDBPyConnection, load: list[str] = (), tpch: float | None = None) -> None:
    """Fill a writable database with TPC-H data (`tpch` scale factor) and a table per `load` file."""
    for path in load:
        if not Path(path).exists():
            raise FileNotFoundError(f"--load file not found: {path}")
    if tpch is not None:
        conn.execute("INSTALL tpch")
        conn.execute("LOAD tpch")
        conn.execute(f"CALL dbgen(sf={float(tpch)})")
    for path in load:
        literal = path.replace("'", "''")
        conn.execute(f'CREATE OR REPLACE TABLE "{table_name_for(path)}" AS SELECT * FROM \'{literal}\'')


def build_database(path: str, load: list[str] = (), tpch: float | None = None) -> None:
    """Create a persistent DuckDB file from `load` files and/or TPC-H data. A failed build
    leaves no half-written file behind."""
    conn = duckdb.connect(path)
    try:
        populate(conn, load, tpch)
    except BaseException:
        conn.close()
        for leftover in (path, path + ".wal"):
            Path(leftover).unlink(missing_ok=True)
        raise
    conn.close()


def connect(db: str, schema: str | None, load: list[str] = (), tpch: float | None = None) -> duckdb.DuckDBPyConnection:
    """Open the database for the agent.

    A file or ducklake: URI is attached READ_ONLY, so DuckDB rejects writes whatever SQL
    Claude sends. ":memory:" can't be read-only (it starts empty), so it is populated
    (see `populate`) and the SQL read-only check is the guard.
    """
    conn = duckdb.connect()
    if db == IN_MEMORY:
        populate(conn, load, tpch)
        if schema:
            conn.execute(f'USE "{schema}"')
        return conn
    conn.execute(f"ATTACH '{db.replace(chr(39), chr(39) * 2)}' AS source (READ_ONLY)")
    conn.execute("USE source" + (f'."{schema}"' if schema else ""))
    return conn


def open_traces_shell(traces_db: str, port: int) -> int:
    """Replace this process with the duckdb CLI, connected to the traces server."""
    try:
        start_traces_server(traces_db, port)
    except TracesServerError as e:
        print(f"Traces server unavailable: {e}", file=sys.stderr)
        return 1
    init_script = str(state_paths(traces_db)["sql"])
    duckdb_cli = shutil.which("duckdb")
    if not duckdb_cli:
        print(f"duckdb CLI not found on PATH. Install it, then run: duckdb -init {init_script}", file=sys.stderr)
        return 1
    os.execv(duckdb_cli, [duckdb_cli, "-init", init_script])


def main(argv=None, client=None) -> int:
    args = parse_args(argv)
    load_dotenv()  # ANTHROPIC_API_KEY from .env
    configure_logging(verbose=not args.quiet)

    if args.stop_traces_server:
        stopped = stop_traces_server(args.traces_db)
        print(f"Traces server {'stopped' if stopped else 'was not running'}.", file=sys.stderr)
        return 0
    if args.traces_shell:
        return open_traces_shell(args.traces_db, args.traces_port)

    if args.save_db:
        build_database(args.save_db, args.load, args.tpch)
        print(f"Saved database: {args.save_db} (reuse it with --db {args.save_db})", file=sys.stderr)
        if args.no_run:
            return 0
        conn = connect(args.db, args.schema)
    else:
        conn = connect(args.db, args.schema, args.load, args.tpch)
    if args.reset_traces and reset_traces_db(args.traces_db):
        print(f"Reset traces: deleted {args.traces_db}", file=sys.stderr)
    traces_conn = connect_traces_db(args.traces_db, server=not args.no_traces_server, port=args.traces_port)
    config = AgentConfig(
        model=args.model,
        effort=args.effort,
        max_iterations=args.max_iterations,
        structured_answer=args.structured,
    )
    result = run_agent(
        conn, args.prompt, tables=args.tables, config=config, traces_conn=traces_conn, client=client
    )
    if args.quiet:
        print(result.text)  # verbose mode already logged it
    return 0 if result.status == "completed" else 1


if __name__ == "__main__":
    sys.exit(main())
