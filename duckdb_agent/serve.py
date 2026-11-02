"""Drive the agent from another program (the desktop app) over JSON Lines on stdin/stdout.

    uv run python -m duckdb_agent.serve [--traces-db PATH] [--no-traces] [--no-traces-server]

Each line on stdin is one request; each line on stdout is one message. Logs go to
stderr, so stdout carries only the protocol. Every request has an "id" (any string the
caller picks) and a "type"; every message names the request it belongs to by "id".

Requests:
    {"id": "h", "type": "hello"}
        -> {"id": "h", "type": "hello", "protocol": 1, "defaults": {...AgentConfig fields},
            "effort_levels": [...], "priced_models": [...], "limits": {"max_tokens": ...},
            "tools": [...]}
    {"id": "r1", "type": "run", "prompt": "...",
     "source": {"db": "tpch.duckdb", "schema": null} | {"load": ["a.csv"], "tpch": 0.1},
     "tables": ["orders"], "config": {"effort": "high", "max_iterations": 10, ...},
     "preset": {"id": "…", "name": "Balanced", "revision": 3, "modified": false},
     "labels": {"setlist": "tpch", "question": "Q3"}}
        -> {"id": "r1", "type": "accepted"}
        -> {"id": "r1", "type": "event", "event": "<name>", "run_id": ..., ...}  (many; see
           EventCallback in loop.py)
        -> {"id": "r1", "type": "result", "result": {...AgentResult fields}}
    {"id": "s", "type": "stop", "run": "r1"}       -> {"id": "s", "type": "ok"}
    {"id": "q", "type": "shutdown"}                -> {"id": "q", "type": "ok"}, then exit
"preset" and "labels" are optional and don't change the run; they're recorded with it in
agent_runs (see run_agent). A bad request gets {"id": ..., "type": "error", "error": "..."}; a run that fails before
it starts (a missing file, a bad db path) gets the same in place of its result.

Runs execute in parallel threads (for A/B), each on its own DuckDB connection. Data is
opened once per distinct source and shared: in-memory data (loaded files, TPC-H) is
built on first use and kept for later runs. End of input stops all runs and exits.
"""

import argparse
import dataclasses
import json
import sys
import threading

import anthropic
import duckdb
from dotenv import load_dotenv

from .config import API_MAX_RETRIES, DEFAULT_CONFIG, EFFORT_LEVELS, MAX_NON_STREAMING_TOKENS, TOOL_NAMES, AgentConfig
from .log import configure_logging, logger
from .loop import run_agent
from .serialization import json_dumps
from .sources import IN_MEMORY, connect, use_statement
from .traces_server import DEFAULT_PORT, connect_traces_db
from .usage import MODEL_PRICING


PROTOCOL_VERSION = 1


CONFIG_FIELDS = {f.name for f in dataclasses.fields(AgentConfig)}


SOURCE_FIELDS = {"db", "schema", "load", "tpch"}


class RequestError(ValueError):
    """A request the server can't act on; reported to the caller as an error message."""


def parse_source(source) -> dict:
    """Normalize a run's "source" into {db, schema, load, tpch}, with the CLI's rules:
    loaded files and TPC-H live in memory; a db file or ducklake: URI is attached read-only."""
    if not isinstance(source, dict):
        raise RequestError('"source" must be an object, e.g. {"db": "tpch.duckdb"}')
    unknown = set(source) - SOURCE_FIELDS
    if unknown:
        raise RequestError(f"unknown source fields: {', '.join(sorted(unknown))}")
    load = source.get("load") or []
    if not isinstance(load, list) or not all(isinstance(path, str) for path in load):
        raise RequestError('"load" must be a list of file paths')
    tpch = source.get("tpch")
    if tpch is not None and (not isinstance(tpch, (int, float)) or isinstance(tpch, bool) or tpch <= 0):
        raise RequestError('"tpch" must be a positive scale factor')
    db = source.get("db")
    if load or tpch is not None:
        if db not in (None, IN_MEMORY):
            raise RequestError('"load" and "tpch" build in-memory data; leave out "db"')
        db = IN_MEMORY
    elif not isinstance(db, str) or not db:
        raise RequestError('"source" needs "db", "load", or "tpch"')
    schema = source.get("schema")
    if schema is not None and not isinstance(schema, str):
        raise RequestError('"schema" must be a string')
    return {"db": db, "schema": schema, "load": list(load), "tpch": tpch}


def parse_config(config) -> AgentConfig:
    """AgentConfig from a run's "config" object. Unknown keys are an error, so a typo in
    the app shows up instead of silently running with a default."""
    if config is None:
        return DEFAULT_CONFIG
    if not isinstance(config, dict):
        raise RequestError('"config" must be an object of AgentConfig fields')
    unknown = set(config) - CONFIG_FIELDS
    if unknown:
        raise RequestError(f"unknown config fields: {', '.join(sorted(unknown))}")
    try:
        return AgentConfig(**config)
    except (TypeError, ValueError) as e:
        raise RequestError(f"invalid config: {e}") from None


PRESET_FIELDS = {"id": str, "name": str, "revision": int, "modified": bool}


MAX_LABELS = 20


def parse_preset(preset) -> dict | None:
    """The run's "preset": which saved settings it started from. Only these fields, so
    the agent_runs.preset column stays queryable."""
    if preset is None:
        return None
    if not isinstance(preset, dict):
        raise RequestError('"preset" must be an object like {"name": "Balanced", "revision": 3}')
    unknown = set(preset) - set(PRESET_FIELDS)
    if unknown:
        raise RequestError(f"unknown preset fields: {', '.join(sorted(unknown))}")
    for name, kind in PRESET_FIELDS.items():
        value = preset.get(name)
        # bool is an int in Python; a revision of True is a mistake.
        if value is not None and (not isinstance(value, kind) or (kind is int and isinstance(value, bool))):
            raise RequestError(f'preset "{name}" must be a {kind.__name__}')
    if not isinstance(preset.get("name"), str) or not preset["name"].strip():
        raise RequestError('"preset" needs a "name"')
    return dict(preset)


def parse_labels(labels) -> dict | None:
    """The run's "labels": the caller's own tags, as a flat object of short values."""
    if labels is None:
        return None
    if not isinstance(labels, dict):
        raise RequestError('"labels" must be an object of tags, e.g. {"question": "Q3"}')
    if len(labels) > MAX_LABELS:
        raise RequestError(f'"labels" can have at most {MAX_LABELS} tags')
    for key, value in labels.items():
        if not key or len(key) > 64:
            raise RequestError("label names must be 1-64 characters")
        if not isinstance(value, (str, int, float, bool)) and value is not None:
            raise RequestError(f'label "{key}" must be a string, number, true/false, or null')
        if isinstance(value, str) and len(value) > 200:
            raise RequestError(f'label "{key}" is longer than 200 characters')
    return dict(labels) or None


class Server:
    """The request loop. `client` and `traces` are injectable for tests: `traces` is a
    zero-argument function returning a TracesDB for one run (or None for no traces)."""

    def __init__(self, stdin, stdout, client=None, traces=None):
        self.stdin = stdin
        self.stdout = stdout
        self._client = client
        self._client_lock = threading.Lock()
        self.traces = traces or (lambda: None)
        self._write_lock = threading.Lock()
        self._sources = {}  # source key -> open base connection
        self._sources_lock = threading.Lock()
        self._runs = {}  # request id -> {"thread", "cancel"}
        self._runs_lock = threading.Lock()

    @property
    def client(self) -> anthropic.Anthropic:
        """Created on first run, so the server starts (and answers hello) without an API key."""
        with self._client_lock:
            if self._client is None:
                self._client = anthropic.Anthropic(max_retries=API_MAX_RETRIES)
            return self._client

    # --- output ---

    def send(self, message: dict) -> None:
        line = json_dumps(message)
        with self._write_lock:
            self.stdout.write(line + "\n")
            self.stdout.flush()

    def send_error(self, request_id, error: str) -> None:
        self.send({"id": request_id, "type": "error", "error": error})

    # --- data ---

    def open_source(self, source: dict) -> duckdb.DuckDBPyConnection:
        """A fresh connection to `source` for one run. The data is opened (or built) once
        per source and shared: cursor() is a new connection to the same database."""
        key = json.dumps(source, sort_keys=True)
        with self._sources_lock:
            base = self._sources.get(key)
            if base is None:
                base = connect(source["db"], source["schema"], source["load"], source["tpch"])
                self._sources[key] = base
        conn = base.cursor()
        use = use_statement(source["db"], source["schema"])
        if use:
            conn.execute(use)
        return conn

    # --- requests ---

    def handle(self, request: dict) -> bool:
        """Act on one request. Returns False when the server should exit."""
        request_id = request.get("id")
        kind = request.get("type")
        if not isinstance(request_id, str) or not request_id:
            self.send_error(request_id, 'every request needs a string "id"')
        elif kind == "hello":
            self.send({
                "id": request_id,
                "type": "hello",
                "protocol": PROTOCOL_VERSION,
                "defaults": dataclasses.asdict(DEFAULT_CONFIG),
                "effort_levels": list(EFFORT_LEVELS),
                "priced_models": sorted(MODEL_PRICING),
                # For a UI's controls: the largest safe max_tokens, and the tools that
                # context_protected_tools can name.
                "limits": {"max_tokens": MAX_NON_STREAMING_TOKENS},
                "tools": list(TOOL_NAMES),
            })
        elif kind == "run":
            self.start_run(request)
        elif kind == "stop":
            with self._runs_lock:
                run = self._runs.get(request.get("run"))
            if run is None:
                self.send_error(request_id, f"no active run with id {request.get('run')!r}")
            else:
                run["cancel"].set()
                self.send({"id": request_id, "type": "ok"})
        elif kind == "shutdown":
            self.stop_all()
            self.send({"id": request_id, "type": "ok"})
            return False
        else:
            self.send_error(request_id, f"unknown request type {kind!r}")
        return True

    def start_run(self, request: dict) -> None:
        request_id = request["id"]
        try:
            prompt = request.get("prompt")
            if not isinstance(prompt, str) or not prompt.strip():
                raise RequestError('"prompt" must be a non-empty string')
            source = parse_source(request.get("source"))
            config = parse_config(request.get("config"))
            tables = request.get("tables")
            if tables is not None and (not isinstance(tables, list) or not all(isinstance(t, str) for t in tables)):
                raise RequestError('"tables" must be a list of table names')
            recorded = {"preset": parse_preset(request.get("preset")), "labels": parse_labels(request.get("labels"))}
        except RequestError as e:
            self.send_error(request_id, str(e))
            return

        cancel = threading.Event()
        with self._runs_lock:
            if request_id in self._runs:
                self.send_error(request_id, f"a run with id {request_id!r} is already active")
                return
            thread = threading.Thread(
                target=self.execute_run, args=(request_id, prompt, source, tables, config, cancel, recorded),
                name=f"run-{request_id}", daemon=True
            )
            self._runs[request_id] = {"thread": thread, "cancel": cancel}
        self.send({"id": request_id, "type": "accepted"})
        thread.start()

    def execute_run(self, request_id, prompt, source, tables, config, cancel, recorded=None) -> None:
        conn = traces_conn = None
        try:
            try:
                conn = self.open_source(source)
                traces_conn = self.traces()
                client = self.client
            except Exception as e:
                logger.warning("Run %s could not start: %s", request_id, e)
                self.send_error(request_id, f"{type(e).__name__}: {e}")
                return
            result = run_agent(
                conn, prompt, tables=tables, config=config, traces_conn=traces_conn, client=client,
                on_event=lambda event: self.send({"id": request_id, "type": "event", **event}),
                cancel=cancel, **(recorded or {})
            )
            self.send({"id": request_id, "type": "result", "result": dataclasses.asdict(result)})
        except Exception as e:
            # run_agent already wrote its summary (and a run_end event); say it ended.
            logger.exception("Run %s failed", request_id)
            self.send_error(request_id, f"{type(e).__name__}: {e}")
        finally:
            for closable in (conn, traces_conn):
                if closable is not None:
                    try:
                        closable.close()
                    except Exception:
                        pass
            with self._runs_lock:
                self._runs.pop(request_id, None)

    def wait_all(self, timeout: float = 30) -> None:
        """Wait for every active run to finish."""
        with self._runs_lock:
            threads = [run["thread"] for run in self._runs.values()]
        for thread in threads:
            thread.join(timeout)

    def stop_all(self, timeout: float = 30) -> None:
        """Cancel every active run and wait for them to finish."""
        with self._runs_lock:
            runs = list(self._runs.values())
        for run in runs:
            run["cancel"].set()
        self.wait_all(timeout)

    def serve_forever(self) -> None:
        for line in self.stdin:
            line = line.strip()
            if not line:
                continue
            try:
                request = json.loads(line)
            except json.JSONDecodeError as e:
                self.send_error(None, f"invalid JSON: {e}")
                continue
            if not isinstance(request, dict):
                self.send_error(None, "each request must be a JSON object")
                continue
            if not self.handle(request):
                return
        self.stop_all()  # end of input: the app went away


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Serve the agent over JSON Lines on stdin/stdout.")
    parser.add_argument("--traces-db", default="agent_traces.duckdb", help="Where to write traces.")
    parser.add_argument("--no-traces", action="store_true", help="Don't write traces.")
    parser.add_argument(
        "--no-traces-server", action="store_true",
        help="Write the traces file directly instead of through the background Quack server."
    )
    parser.add_argument("--traces-port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--verbose", action="store_true", help="Log SQL, thinking, and answers to stderr.")
    args = parser.parse_args(argv)
    load_dotenv()  # ANTHROPIC_API_KEY from .env
    configure_logging(verbose=args.verbose, stream=sys.stderr)
    traces = None
    if not args.no_traces:
        def traces():
            return connect_traces_db(args.traces_db, server=not args.no_traces_server, port=args.traces_port)
    Server(sys.stdin, sys.stdout, traces=traces).serve_forever()
    return 0


if __name__ == "__main__":
    sys.exit(main())
