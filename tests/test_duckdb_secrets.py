"""DuckDB secrets for the settings screen: SQL built safely, values never sent back.

Every test points DuckDB at a temporary secret directory, never ~/.duckdb/stored_secrets.
"""

import duckdb
import pytest

from duckdb_agent import duckdb_secrets
from duckdb_agent.duckdb_secrets import SecretError, create_secret, create_statement, drop_secret, list_secrets, redact
from test_serve import server


@pytest.fixture
def secret_dir(tmp_path, monkeypatch):
    path = str(tmp_path / "stored_secrets")
    monkeypatch.setattr(duckdb_secrets, "connect", lambda: duckdb.connect(config={"secret_directory": path}))
    return path


@pytest.fixture
def conn(secret_dir):
    c = duckdb_secrets.connect()
    yield c
    c.close()


S3 = {"KEY_ID": "AKIAEXAMPLE", "SECRET": "s3cr3t/with'quote"}


# --- the statement ---

def test_statement_quotes_every_value():
    sql, used = create_statement("lake", "s3", {**S3, "REGION": "us-east-1"}, scope="s3://bucket")
    assert sql == (
        "CREATE PERSISTENT SECRET lake (TYPE s3, KEY_ID 'AKIAEXAMPLE', SECRET 's3cr3t/with''quote', "
        "REGION 'us-east-1', SCOPE 's3://bucket')"
    )
    assert "s3cr3t/with'quote" in used


def test_statement_skips_empty_optional_fields_and_replaces():
    sql, _ = create_statement("lake", "s3", {**S3, "REGION": "  ", "ENDPOINT": ""}, replace=True)
    assert sql.startswith("CREATE OR REPLACE PERSISTENT SECRET lake (TYPE s3, KEY_ID")
    assert "REGION" not in sql and "ENDPOINT" not in sql


def test_ducklake_catalog_secret_goes_in_metadata_parameters():
    existing = [{"name": "pg_meta", "type": "postgres", "provider": "config", "scope": []}]
    sql, _ = create_statement("lake", "ducklake", {"DATA_PATH": "s3://b/lake/", "SECRET": "pg_meta", "SCHEMA": "lake"},
                              existing=existing)
    assert "DATA_PATH 's3://b/lake/'" in sql
    assert "METADATA_PARAMETERS MAP {'TYPE': 'postgres', 'SECRET': 'pg_meta', 'SCHEMA': 'lake'}" in sql


@pytest.mark.parametrize("name, secret_type, values, kwargs, problem", [
    ("1bad", "s3", S3, {}, "letters, digits"),
    ("x; DROP", "s3", S3, {}, "letters, digits"),
    ("ok", "smb", S3, {}, "Unknown secret type"),
    ("ok", "s3", {"KEY_ID": "a"}, {}, "Secret access key is required"),
    ("ok", "s3", {**S3, "PASSWORD": "x"}, {}, "don't take PASSWORD"),
    ("ok", "postgres", {"HOST": "h", "DATABASE": "d", "USER": "u", "PORT": "54x"}, {}, "whole number"),
    ("ok", "postgres", {"HOST": "h", "DATABASE": "d", "USER": "u"}, {"scope": "x"}, "don't take a scope"),
    ("ok", "ducklake", {"DATA_PATH": "p", "SECRET": "nope"}, {}, "no postgres secret named nope"),
])
def test_statement_refuses(name, secret_type, values, kwargs, problem):
    with pytest.raises(SecretError, match=problem):
        create_statement(name, secret_type, values, **kwargs)


def test_redact_blanks_values():
    assert redact("bad value 'hunter2' near hunter", ["hunter2", "hunter"]) == "bad value '•••' near •••"


# --- against DuckDB, in a temporary secret directory ---

def test_create_list_and_drop(conn, secret_dir):
    create_secret(conn, "lake", "s3", S3, scope="s3://bucket")
    (secret,) = list_secrets(conn)
    assert secret == {"name": "lake", "type": "s3", "provider": "config", "scope": ["s3://bucket"]}
    # It's persistent: a new connection to the same directory sees it.
    assert [s["name"] for s in list_secrets(duckdb_secrets.connect())] == ["lake"]
    drop_secret(conn, "lake")
    assert list_secrets(conn) == []


def test_create_refuses_a_taken_name_unless_replacing(conn):
    create_secret(conn, "lake", "s3", S3)
    with pytest.raises(SecretError, match="already a secret named lake"):
        create_secret(conn, "lake", "s3", S3)
    create_secret(conn, "lake", "s3", {**S3, "REGION": "eu-west-1"}, replace=True)
    assert len(list_secrets(conn)) == 1


def test_drop_refuses_an_unknown_name(conn):
    with pytest.raises(SecretError, match="no saved secret named ghost"):
        drop_secret(conn, "ghost")


def test_postgres_and_ducklake_secrets_are_created(conn):
    create_secret(conn, "pg_meta", "postgres", {"HOST": "localhost", "PORT": "5432", "DATABASE": "meta", "USER": "u", "PASSWORD": "pw"})
    create_secret(conn, "lake", "ducklake", {"DATA_PATH": "/tmp/lake/", "SECRET": "pg_meta"})
    assert [(s["name"], s["type"]) for s in list_secrets(conn)] == [("lake", "ducklake"), ("pg_meta", "postgres")]


# --- the server ---

def test_server_lists_creates_and_drops_without_echoing_values(secret_dir):
    srv, out = server()
    srv.handle({"id": "c", "type": "secret_create", "name": "lake", "secret_type": "s3", "values": S3, "scope": None})
    (created,) = out.of("c")
    assert created["type"] == "secrets"
    assert created["secrets"] == [{"name": "lake", "type": "s3", "provider": "config", "scope": ["s3://", "s3n://", "s3a://"]}]
    assert S3["SECRET"] not in str(out.messages) and S3["KEY_ID"] not in str(out.messages)

    srv.handle({"id": "l", "type": "secrets"})
    assert [s["name"] for s in out.of("l")[0]["secrets"]] == ["lake"]

    srv.handle({"id": "d", "type": "secret_drop", "name": "lake"})
    assert out.of("d")[0]["secrets"] == []


def test_server_reports_a_bad_request_as_an_error(secret_dir):
    srv, out = server()
    srv.handle({"id": "c", "type": "secret_create", "name": "lake", "secret_type": "s3", "values": {"KEY_ID": "a"}})
    (error,) = out.of("c")
    assert error == {"id": "c", "type": "error", "error": "Secret access key is required."}


def test_hello_lists_secret_types():
    srv, out = server()
    srv.handle({"id": "h", "type": "hello"})
    types = {t["type"] for t in out.messages[0]["secret_types"]}
    assert {"s3", "postgres", "ducklake"} <= types
