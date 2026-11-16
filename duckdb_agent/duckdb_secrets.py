"""DuckDB secrets: list, create, and drop the persistent ones, for a settings screen.

Persistent secrets live in DuckDB's own store (~/.duckdb/stored_secrets by default), so
the agent, the duckdb CLI, and any other DuckDB program on this machine see the same
ones, and every new connection loads them. DuckDB keeps them unencrypted, readable only by
this user. Values go into DuckDB and nowhere else: they're never returned, logged, or put
in an error message (see redact).

SECRET_TYPES is what a UI builds its forms from (hello sends it): each type's fields, in
order, with how to show them. create_secret turns a form's values into one
CREATE PERSISTENT SECRET statement, quoting every value.
"""

import re

import duckdb


def connect() -> duckdb.DuckDBPyConnection:
    """A connection that sees this machine's persistent secrets (tests point it elsewhere)."""
    return duckdb.connect()

NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# kind: "text" | "password" (hidden as it's typed) | "int" | "secret" (the name of another
# secret, of `secret_type`). A field with "map" goes into a MAP parameter instead of its
# own (DuckLake's METADATA_PARAMETERS); one with "always" is sent even when empty (DuckDB
# requires DuckLake's METADATA_PATH, empty for a Postgres catalog). `scope`: whether the
# type takes a SCOPE prefix.
SECRET_TYPES = [
    {
        "type": "s3", "label": "S3", "scope": True, "scope_hint": "s3://bucket",
        "fields": [
            {"key": "KEY_ID", "label": "Access key ID", "kind": "text", "required": True},
            {"key": "SECRET", "label": "Secret access key", "kind": "password", "required": True},
            {"key": "REGION", "label": "Region", "kind": "text", "placeholder": "us-east-1"},
            {"key": "SESSION_TOKEN", "label": "Session token", "kind": "password"},
            {"key": "ENDPOINT", "label": "Endpoint", "kind": "text", "placeholder": "for S3-compatible storage"},
            {"key": "URL_STYLE", "label": "URL style", "kind": "text", "placeholder": "vhost or path"},
        ],
    },
    {
        "type": "r2", "label": "Cloudflare R2", "scope": True, "scope_hint": "r2://bucket",
        "fields": [
            {"key": "KEY_ID", "label": "Access key ID", "kind": "text", "required": True},
            {"key": "SECRET", "label": "Secret access key", "kind": "password", "required": True},
            {"key": "ACCOUNT_ID", "label": "Account ID", "kind": "text", "required": True},
        ],
    },
    {
        "type": "gcs", "label": "Google Cloud Storage", "scope": True, "scope_hint": "gs://bucket",
        "fields": [
            {"key": "KEY_ID", "label": "HMAC key ID", "kind": "text", "required": True},
            {"key": "SECRET", "label": "HMAC secret", "kind": "password", "required": True},
        ],
    },
    {
        "type": "postgres", "label": "Postgres", "scope": False,
        "fields": [
            {"key": "HOST", "label": "Host", "kind": "text", "required": True},
            {"key": "PORT", "label": "Port", "kind": "int", "placeholder": "5432"},
            {"key": "DATABASE", "label": "Database", "kind": "text", "required": True},
            {"key": "USER", "label": "User", "kind": "text", "required": True},
            {"key": "PASSWORD", "label": "Password", "kind": "password"},
        ],
    },
    {
        "type": "mysql", "label": "MySQL", "scope": False,
        "fields": [
            {"key": "HOST", "label": "Host", "kind": "text", "required": True},
            {"key": "PORT", "label": "Port", "kind": "int", "placeholder": "3306"},
            {"key": "DATABASE", "label": "Database", "kind": "text", "required": True},
            {"key": "USER", "label": "User", "kind": "text", "required": True},
            {"key": "PASSWORD", "label": "Password", "kind": "password"},
        ],
    },
    {
        "type": "ducklake", "label": "DuckLake", "scope": False,
        "fields": [
            {"key": "DATA_PATH", "label": "Data path", "kind": "text", "required": True, "placeholder": "s3://bucket/lake/"},
            {"key": "METADATA_PATH", "label": "Metadata path", "kind": "text", "always": True,
             "placeholder": "a .ducklake file, or empty with a Postgres catalog"},
            {"key": "SECRET", "label": "Catalog's Postgres secret", "kind": "secret", "secret_type": "postgres",
             "map": "METADATA_PARAMETERS"},
            {"key": "SCHEMA", "label": "Catalog schema", "kind": "text", "map": "METADATA_PARAMETERS"},
        ],
    },
    {
        "type": "http", "label": "HTTP", "scope": True, "scope_hint": "https://api.example.com",
        "fields": [
            {"key": "BEARER_TOKEN", "label": "Bearer token", "kind": "password", "required": True},
        ],
    },
]

TYPES = {t["type"]: t for t in SECRET_TYPES}


class SecretError(ValueError):
    """A request to change secrets that can't be done; the message is safe to show."""


def quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def list_secrets(conn: duckdb.DuckDBPyConnection) -> list[dict]:
    """The persistent secrets: name, type, provider, scope. Never their values."""
    rows = conn.execute(
        "SELECT name, type, provider, scope FROM duckdb_secrets() WHERE persistent ORDER BY name"
    ).fetchall()
    return [{"name": n, "type": t, "provider": p, "scope": list(s or [])} for n, t, p, s in rows]


def create_statement(name: str, secret_type: str, values: dict, scope: str | None = None, replace: bool = False,
                     existing: list[dict] | None = None) -> tuple[str, list[str]]:
    """The CREATE PERSISTENT SECRET statement for a form's values, and the values in it
    (to keep out of error messages). Raises SecretError for anything the form got wrong."""
    if not isinstance(name, str) or not NAME.match(name):
        raise SecretError("A secret's name is letters, digits, and underscores, and doesn't start with a digit.")
    spec = TYPES.get(secret_type)
    if spec is None:
        raise SecretError(f"Unknown secret type {secret_type!r}.")
    if not isinstance(values, dict):
        raise SecretError("values must be an object.")
    unknown = set(values) - {f["key"] for f in spec["fields"]}
    if unknown:
        raise SecretError(f"{spec['label']} secrets don't take {', '.join(sorted(unknown))}.")
    names = {s["name"] for s in existing or []}
    if not replace and name in names:
        raise SecretError(f"There's already a secret named {name}.")

    params = [f"TYPE {secret_type}"]
    maps: dict[str, list[str]] = {}
    used: list[str] = []
    for field in spec["fields"]:
        raw = values.get(field["key"])
        value = raw.strip() if isinstance(raw, str) else raw
        if value in (None, ""):
            if field.get("required"):
                raise SecretError(f"{field['label']} is required.")
            if field.get("always"):
                params.append(f"{field['key']} ''")
            continue
        if field["kind"] == "int":
            if isinstance(value, bool) or not str(value).isdigit():
                raise SecretError(f"{field['label']} must be a whole number.")
            literal = str(int(value))
        elif not isinstance(value, str):
            raise SecretError(f"{field['label']} must be text.")
        elif field["kind"] == "secret":
            matches = [s for s in existing or [] if s["name"] == value and s["type"] == field["secret_type"]]
            if not NAME.match(value) or not matches:
                raise SecretError(f"{field['label']}: there's no {field['secret_type']} secret named {value}.")
            literal = quote(value)
        else:
            literal = quote(value)
            used.append(value)
        if "map" in field:
            maps.setdefault(field["map"], []).append(f"{quote(field['key'])}: {literal}")
        else:
            params.append(f"{field['key']} {literal}")
    for key, entries in maps.items():
        # DuckLake's catalog parameters: a Postgres secret means a Postgres catalog.
        if key == "METADATA_PARAMETERS":
            entries.insert(0, "'TYPE': 'postgres'")
        params.append(f"{key} MAP {{{', '.join(entries)}}}")
    if scope:
        if not spec["scope"]:
            raise SecretError(f"{spec['label']} secrets don't take a scope.")
        if not isinstance(scope, str):
            raise SecretError("scope must be text.")
        params.append(f"SCOPE {quote(scope.strip())}")
    head = "CREATE OR REPLACE PERSISTENT SECRET" if replace else "CREATE PERSISTENT SECRET"
    return f"{head} {name} ({', '.join(params)})", used


def redact(message: str, values: list[str]) -> str:
    """An error message with the given values blanked, longest first."""
    for value in sorted({v for v in values if v}, key=len, reverse=True):
        message = message.replace(value, "•••")
    return message


def create_secret(conn: duckdb.DuckDBPyConnection, name: str, secret_type: str, values: dict,
                  scope: str | None = None, replace: bool = False) -> None:
    """Save a persistent secret. Raises SecretError, never with a value in its message."""
    sql, used = create_statement(name, secret_type, values, scope, replace, existing=list_secrets(conn))
    try:
        conn.execute(sql)
    except duckdb.Error as e:
        raise SecretError(redact(str(e), used)) from None


def drop_secret(conn: duckdb.DuckDBPyConnection, name: str) -> None:
    """Delete a persistent secret by name."""
    if not isinstance(name, str) or not NAME.match(name):
        raise SecretError("Not a secret's name.")
    if name not in {s["name"] for s in list_secrets(conn)}:
        raise SecretError(f"There's no saved secret named {name}.")
    try:
        conn.execute(f"DROP PERSISTENT SECRET {name}")
    except duckdb.Error as e:
        raise SecretError(str(e)) from None
