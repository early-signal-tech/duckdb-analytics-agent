"""A background Quack server that owns the traces database, so runs can write traces
while other processes (a duckdb CLI in another terminal) read them.

DuckDB lets one process at a time open a file for writing. The server is that process:
it opens the file, serves it over Quack on localhost, and keeps running until stopped.
Runs and readers connect to it as clients.

    traces = connect_traces_db("agent_traces.duckdb")   # starts the server if needed
    stop_traces_server("agent_traces.duckdb")

Next to the database the server keeps, readable only by you:
    <db>.quack.json   pid, URI, and token, for clients
    <db>.quack.sql    init script for the CLI: duckdb -init agent_traces.duckdb.quack.sql
    <db>.quack.log    the server's output, for when it fails to start

Run in the foreground with: python -m duckdb_agent.traces_server PATH [--port PORT]
"""

import argparse
import json
import os
import secrets
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

import duckdb

from .log import logger
from .tracing import TracesDB, create_traces_schema, init_traces_db


DEFAULT_PORT = 9494  # Quack's default


# How long a client waits for a server it started to come up. Starting includes
# loading the quack extension, which downloads on first use.
SERVER_START_TIMEOUT_SECONDS = 30


# Run with -c rather than -m: the package imports this module, and -m on an
# already-imported module warns. _pid_is_server looks for this text in ps output.
SERVER_COMMAND = "import sys; from duckdb_agent.traces_server import main; sys.exit(main(sys.argv[1:]))"


TOKEN_ENV = "DUCKDB_AGENT_QUACK_TOKEN"  # passes the token to the server without putting it in ps output


class TracesServerError(Exception):
    """The traces server could not be started or reached."""


def state_paths(db_path: str) -> dict:
    db_path = os.path.abspath(db_path)
    return {name: Path(f"{db_path}.quack.{name}") for name in ("json", "sql", "log")}


def _write_private(path: Path, text: str) -> None:
    """Write a file only the owner can read: it holds the token."""
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(text)


def _read_state(db_path: str) -> dict | None:
    try:
        return json.loads(state_paths(db_path)["json"].read_text())
    except (OSError, ValueError):
        return None


def _query(uri: str, token: str, sql: str):
    """Run one statement on a Quack server and return its rows."""
    with duckdb.connect() as conn:
        return conn.execute("SELECT * FROM quack_query(?, ?, token = ?)", [uri, sql, token]).fetchall()


def server_status(db_path: str) -> dict | None:
    """The state of the running server for this database ({pid, uri, token, db}), or
    None if none is answering. A server is only trusted if it says it serves this file."""
    state = _read_state(db_path)
    if not state:
        return None
    try:
        rows = _query(state["uri"], state["token"],
                      "SELECT path FROM duckdb_databases() WHERE database_name = current_database()")
    except (duckdb.Error, KeyError):
        return None
    if not rows or os.path.realpath(rows[0][0]) != os.path.realpath(db_path):
        return None
    return state


def _pid_is_server(pid: int) -> bool:
    """True if `pid` is a running traces server, so a stale pidfile never kills a stranger."""
    try:
        command = subprocess.run(["ps", "-p", str(pid), "-o", "command="],
                                 capture_output=True, text=True, check=False).stdout
    except OSError:
        return False
    return "duckdb_agent.traces_server" in command


def start_traces_server(db_path: str, port: int = DEFAULT_PORT) -> dict:
    """Return the running server for `db_path`, starting one in the background if none
    is answering. Raises TracesServerError if it can't be started."""
    state = server_status(db_path)
    if state:
        return state
    paths = state_paths(db_path)
    package_root = str(Path(__file__).resolve().parent.parent)
    env = {
        **os.environ,
        TOKEN_ENV: secrets.token_urlsafe(32),
        "PYTHONPATH": os.pathsep.join(filter(None, [package_root, os.environ.get("PYTHONPATH")])),
    }
    with open(paths["log"], "ab") as log:
        # A new session, so the server outlives this process and the terminal it ran in.
        process = subprocess.Popen(
            [sys.executable, "-c", SERVER_COMMAND, os.path.abspath(db_path), "--port", str(port)],
            env=env, stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT, start_new_session=True,
        )
    deadline = time.monotonic() + SERVER_START_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        # Checked before the exit code: if another run started a server at the same
        # moment, ours exits (the file is locked) but theirs serves the same file.
        state = server_status(db_path)
        if state:
            return state
        if process.poll() is not None:
            raise TracesServerError(f"traces server exited with status {process.returncode}: {_log_tail(paths['log'])}")
        time.sleep(0.1)
    raise TracesServerError(f"traces server did not answer within {SERVER_START_TIMEOUT_SECONDS}s; see {paths['log']}")


def _log_tail(path: Path, lines: int = 3) -> str:
    try:
        return " | ".join(path.read_text().strip().splitlines()[-lines:])
    except OSError:
        return "(no log)"


def stop_traces_server(db_path: str, timeout_seconds: float = 10) -> bool:
    """Stop the server for `db_path` if one is running. Returns True if one was stopped."""
    paths = state_paths(db_path)
    state = _read_state(db_path)
    stopped = False
    if state and _pid_is_server(state.get("pid", -1)):
        os.kill(state["pid"], signal.SIGTERM)
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline and _pid_is_server(state["pid"]):
            time.sleep(0.05)
        stopped = True
    for name in ("json", "sql"):
        paths[name].unlink(missing_ok=True)
    return stopped


def connect_traces_db(db_path: str = "agent_traces.duckdb", server: bool = True, port: int = DEFAULT_PORT) -> TracesDB:
    """Connect to the traces database, through its Quack server when `server` is set
    (starting it if needed), so other processes can read it while runs write. If the
    server can't be started, falls back to opening the file directly, with a warning."""
    if not server:
        return init_traces_db(db_path)
    try:
        state = start_traces_server(db_path, port)
        conn = duckdb.connect()
        conn.execute("ATTACH ? AS traces (TYPE quack, TOKEN ?)", [state["uri"], state["token"]])
        conn.execute("USE traces")
    except (TracesServerError, duckdb.Error) as e:
        logger.warning("Traces server unavailable (%s); writing %s directly, so other processes "
                       "can't read it during the run.", e, db_path)
        return init_traces_db(db_path)
    logger.info("Traces: %s (server pid %s). Read them with: duckdb -init %s",
                state["uri"], state["pid"], state_paths(db_path)["sql"])
    return TracesDB(conn, state["uri"], state["token"])


def serve(db_path: str, port: int, token: str) -> None:
    """Open `db_path`, serve it on localhost until SIGTERM/SIGINT, then shut down cleanly."""
    conn = duckdb.connect(db_path)
    create_traces_schema(conn)
    uri = f"quack:localhost:{port}"
    conn.execute("CALL quack_serve(?, token = ?)", [uri, token])  # serves on background threads
    paths = state_paths(db_path)
    _write_private(paths["json"], json.dumps({"pid": os.getpid(), "uri": uri, "token": token, "db": db_path}))
    _write_private(paths["sql"], (
        "-- Connects the duckdb CLI to the traces server: duckdb -init <this file>\n"
        f"ATTACH '{uri}' AS traces (TYPE quack, TOKEN '{token}');\n"
        "CONNECT traces;\n"
    ))
    print(f"Serving {db_path} on {uri} (pid {os.getpid()})", flush=True)

    stop = threading.Event()
    for signum in (signal.SIGTERM, signal.SIGINT):
        signal.signal(signum, lambda *_: stop.set())
    while not stop.wait(1):
        pass
    print("Stopping", flush=True)
    # The quack threads keep the process alive until the server is stopped.
    conn.execute("CALL quack_stop(?)", [uri])
    conn.execute("CHECKPOINT")
    conn.close()
    state = _read_state(db_path)
    if state and state.get("pid") == os.getpid():
        for name in ("json", "sql"):
            paths[name].unlink(missing_ok=True)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Serve a traces database over Quack.")
    parser.add_argument("db", help="Traces database file")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    args = parser.parse_args(argv)
    token = os.environ.get(TOKEN_ENV) or secrets.token_urlsafe(32)
    serve(os.path.abspath(args.db), args.port, token)
    return 0


if __name__ == "__main__":
    sys.exit(main())
