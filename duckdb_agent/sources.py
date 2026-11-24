"""Opening the data the agent queries: DuckDB files (local or remote), ducklake: URIs,
remote data files queried in place (s3://bucket/orders/*.parquet), loaded files, TPC-H,
and the agent's own traces (TRACES_SOURCE), read through the traces server."""

import re
from pathlib import Path

import duckdb


IN_MEMORY = ":memory:"

# A run's "db" that means the agent's own traces database. Its file is held by the traces
# server (Quack), so it's read through that server, never opened directly.
TRACES_SOURCE = "agent:traces"


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
    if db == IN_MEMORY or is_remote_data(db):
        return f'USE "{schema}"' if schema else None
    return "USE source" + (f'."{schema}"' if schema else "")


# Where DuckDB looks for the system's CA certificates (its own list, in its order). HTTPS
# to S3 and the like reads one, so the lock-down allows that one file: public certificates.
CA_BUNDLES = (
    "/etc/ssl/certs/ca-certificates.crt", "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem",
    "/etc/pki/tls/certs/ca-bundle.crt", "/etc/ssl/ca-bundle.pem", "/etc/ssl/cert.pem",
)


def ca_bundle() -> str | None:
    """The first CA certificate bundle that exists, or None."""
    return next((path for path in CA_BUNDLES if Path(path).is_file()), None)


# Data paths that need an extension to read. Once lock_down runs, extensions can't
# autoload, so the one a remote DuckLake needs is loaded first.
REMOTE_EXTENSIONS = {
    "s3://": "httpfs", "s3a://": "httpfs", "r2://": "httpfs", "gs://": "httpfs", "gcs://": "httpfs",
    "http://": "httpfs", "https://": "httpfs", "hf://": "httpfs", "az://": "azure", "abfss://": "azure",
}


def remote_extension(path: str) -> str | None:
    """The extension that reads `path`, if it's remote (s3://... needs httpfs)."""
    return next((ext for prefix, ext in REMOTE_EXTENSIONS.items() if path.lower().startswith(prefix)), None)


DATABASE_SUFFIXES = (".duckdb", ".db", ".ddb")


def is_remote_data(db: str) -> bool:
    """Whether `db` is remote data files to query in place (s3://bucket/orders/*.parquet,
    a folder, or one file), rather than a remote DuckDB file to attach."""
    return remote_extension(db) is not None and not db.lower().rstrip("/").endswith(DATABASE_SUFFIXES)


GLOB = re.compile(r"[*?\[{]")


def data_files(path: str) -> str:
    """What to read for a data path: a folder means every Parquet file under it (the way
    a Parquet destination writes them); a file or a glob is read as given."""
    return path + "**/*.parquet" if path.endswith("/") else path


def data_directory(path: str) -> str:
    """The folder a data path reads from (up to the first glob), for allowed_directories."""
    match = GLOB.search(path)
    head = path[:match.start()] if match else path
    return head if head.endswith("/") else head[: head.rfind("/") + 1]


def view_name_for(path: str) -> str:
    """The view a data path becomes: its file's name (data.parquet -> data), or its
    folder's (s3://bucket/orders/*.parquet, s3://bucket/orders/, and
    s3://bucket/orders/year=2026/*.parquet -> orders)."""
    # Hive partition folders (year=2026) say how the data is split, not what it is.
    parts = [p for p in path.split("://", 1)[-1].rstrip("/").split("/") if p and not GLOB.search(p) and "=" not in p]
    if not parts:
        return "data"
    last = parts[-1]
    if "." in last and not path.rstrip("/").endswith(last + "/"):
        return table_name_for(last)
    return re.sub(r"\W+", "_", last).strip("_").lower() or "data"


def attach_data(conn: duckdb.DuckDBPyConnection, path: str) -> str:
    """Make a view over remote data files, read in place (each query sees the files as
    they are then). Creating it reads the files' schema, so a wrong path or missing
    credentials fails here. Returns the folder it reads from."""
    extension = remote_extension(path)
    if extension:
        conn.execute(f"INSTALL {extension}")
        conn.execute(f"LOAD {extension}")
    literal = data_files(path).replace("'", "''")
    conn.execute(f'CREATE VIEW "{view_name_for(path)}" AS SELECT * FROM \'{literal}\'')
    return data_directory(path)


def lock_down(conn: duckdb.DuckDBPyConnection, allowed: list[str] = ()) -> None:
    """Keep the agent's SQL inside the data it was given.

    READ_ONLY stops writes, and the read-only check allows only SELECTs, but a SELECT
    can still read any file or URL (read_csv('~/.env'), FROM 'https://...'). This turns
    off external access, leaving only the data files of attached DuckLakes reachable,
    and locks the configuration so SQL can't turn it back on. Call it after attaching
    and populating: from here on, nothing can be attached, loaded, or installed.
    Cursors of this connection share the settings. `allowed` adds folders that stay
    readable (remote data files' folders); an attached remote DuckDB file's path does too.
    """
    allowed = list(allowed)
    for (path,) in conn.execute("SELECT path FROM duckdb_databases() WHERE type = 'duckdb' AND path IS NOT NULL").fetchall():
        if remote_extension(path):
            allowed.append(path)
    for (catalog,) in conn.execute("SELECT database_name FROM duckdb_databases() WHERE type = 'ducklake'").fetchall():
        data_path = conn.execute("SELECT data_path FROM ducklake_settings(?)", [catalog]).fetchone()[0]
        if not data_path:
            continue
        allowed.append(data_path)
        extension = remote_extension(data_path)
        if extension:
            conn.execute(f"INSTALL {extension}")
            conn.execute(f"LOAD {extension}")
    # DuckDB reads saved (persistent) secrets from disk the first time one is needed, which
    # for a DuckLake on S3 is the first query that opens a data file: after the lock-down,
    # when it can't read ~/.duckdb/stored_secrets any more. Load them now, while it can;
    # the folder itself stays out of the agent's reach.
    conn.execute("SELECT name FROM duckdb_secrets()").fetchall()
    if allowed:
        conn.execute("SET allowed_directories = ?", [allowed])
    # HTTPS (remote data, a DuckLake on S3) needs the CA certificates: that one file.
    bundle = ca_bundle()
    if bundle:
        conn.execute("SET allowed_paths = ?", [[bundle]])
    conn.execute("SET enable_external_access = false")
    conn.execute("SET lock_configuration = true")


def connect_traces(uri: str, token: str) -> duckdb.DuckDBPyConnection:
    """The traces, attached as `source` through their Quack server (quack:host:port), for
    the agent to query like any database. READ_ONLY is asked for, but a Quack catalog
    doesn't enforce it, so the read-only SQL check (sql.py) is the guard, as for
    in-memory data. Locked down like every source: the server's own address is the only
    thing outside the connection it can reach."""
    host = uri.split(":", 1)[1]  # quack:localhost:9494 -> localhost:9494
    conn = duckdb.connect()
    conn.execute("ATTACH ? AS source (TYPE quack, TOKEN ?, READ_ONLY)", [uri, token])
    lock_down(conn, [f"http://{host}/"])
    return conn


def connect(db: str, schema: str | None, load: list[str] = (), tpch: float | None = None) -> duckdb.DuckDBPyConnection:
    """Open the database for the agent.

    A file or ducklake: URI (local or remote, like s3://bucket/sales.duckdb) is attached
    READ_ONLY, so DuckDB rejects writes whatever SQL Claude sends. Remote data files
    (s3://bucket/orders/*.parquet) become a view in memory, read in place. ":memory:"
    can't be read-only (it starts empty), so it is populated (see `populate`) and the SQL
    read-only check is the guard, as it is for views. Either way the connection is then
    locked down (see `lock_down`), so queries can't read other files or URLs. Remote
    paths use the machine's persistent DuckDB secrets for credentials.
    """
    conn = duckdb.connect()
    allowed: list[str] = []
    if db == IN_MEMORY:
        populate(conn, load, tpch)
    elif is_remote_data(db):
        allowed.append(attach_data(conn, db))
    else:
        conn.execute(f"ATTACH '{db.replace(chr(39), chr(39) * 2)}' AS source (READ_ONLY)")
    use = use_statement(db, schema)
    if use:
        conn.execute(use)
    lock_down(conn, allowed)
    return conn
