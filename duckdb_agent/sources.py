"""Opening the data the agent queries: DuckDB files, ducklake: URIs, loaded files, TPC-H."""

import re
from pathlib import Path

import duckdb


IN_MEMORY = ":memory:"


DEFAULT_TPCH_SCALE = 0.1  # ~100 MB of data, generated in a few seconds


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


def use_statement(db: str, schema: str | None) -> str | None:
    """The USE that points a connection at the agent's data, or None for in-memory data
    without a schema. Each new DuckDB connection (including cursor()) needs it again."""
    if db == IN_MEMORY:
        return f'USE "{schema}"' if schema else None
    return "USE source" + (f'."{schema}"' if schema else "")


def connect(db: str, schema: str | None, load: list[str] = (), tpch: float | None = None) -> duckdb.DuckDBPyConnection:
    """Open the database for the agent.

    A file or ducklake: URI is attached READ_ONLY, so DuckDB rejects writes whatever SQL
    Claude sends. ":memory:" can't be read-only (it starts empty), so it is populated
    (see `populate`) and the SQL read-only check is the guard.
    """
    conn = duckdb.connect()
    if db == IN_MEMORY:
        populate(conn, load, tpch)
    else:
        conn.execute(f"ATTACH '{db.replace(chr(39), chr(39) * 2)}' AS source (READ_ONLY)")
    use = use_statement(db, schema)
    if use:
        conn.execute(use)
    return conn
