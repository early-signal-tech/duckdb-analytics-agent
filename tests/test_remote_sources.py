"""Remote LINE IN paths: an S3 DuckDB file is attached, S3 data files become a view read
in place. These run offline; the view and the lock are checked with a local folder
standing in for the bucket."""

import duckdb
import pytest

from duckdb_agent.sources import (
    attach_data, data_directory, data_files, is_remote_data, lock_down, use_statement, view_name_for,
)
from duckdb_agent.sql import execute_query


@pytest.mark.parametrize("path, remote_data", [
    ("s3://bucket/orders/*.parquet", True),
    ("s3://bucket/orders/", True),
    ("s3://bucket/x/data.csv", True),
    ("https://example.com/data.json", True),
    ("s3://bucket/sales.duckdb", False),  # a DuckDB file: attached
    ("/Users/me/sales.duckdb", False),
    ("ducklake:my_catalog", False),
    (":memory:", False),
])
def test_is_remote_data(path, remote_data):
    assert is_remote_data(path) is remote_data


@pytest.mark.parametrize("path, view", [
    ("s3://bucket/orders/*.parquet", "orders"),
    ("s3://bucket/orders/", "orders"),
    ("s3://bucket/x/Daily Sales.parquet", "daily_sales"),
    ("s3://bucket/lake/orders/year=2026/*/*.parquet", "orders"),
    ("s3://bucket/*.csv", "bucket"),
])
def test_view_name_for(path, view):
    assert view_name_for(path) == view


def test_folders_and_globs():
    assert data_files("s3://bucket/orders/") == "s3://bucket/orders/**/*.parquet"
    assert data_files("s3://bucket/orders/*.csv") == "s3://bucket/orders/*.csv"
    assert data_directory("s3://bucket/orders/*.parquet") == "s3://bucket/orders/"
    assert data_directory("s3://bucket/orders/year=2026/*/*.parquet") == "s3://bucket/orders/year=2026/"
    assert data_directory("s3://bucket/x/data.parquet") == "s3://bucket/x/"


def test_remote_data_runs_in_memory_without_a_use():
    assert use_statement("s3://bucket/orders/*.parquet", None) is None
    assert use_statement("s3://bucket/orders/*.parquet", "main") == 'USE "main"'
    assert use_statement("s3://bucket/sales.duckdb", None) == "USE source"


def test_a_data_view_reads_its_folder_and_nothing_else(tmp_path):
    folder = tmp_path / "orders"
    folder.mkdir()
    writer = duckdb.connect()
    writer.execute(f"COPY (SELECT 1 AS id, 10.0 AS amount) TO '{folder}/a.parquet'")
    writer.execute(f"COPY (SELECT 2 AS id, 20.0 AS amount) TO '{folder}/b.parquet'")
    writer.close()
    secret = tmp_path / "secret.csv"
    secret.write_text("token\nhunter2\n")

    conn = duckdb.connect()
    allowed = attach_data(conn, f"{folder}/*.parquet")
    lock_down(conn, [allowed])
    assert execute_query(conn, "SELECT sum(amount) FROM orders")["rows"] == [[30.0]]
    # A file written later is seen: the view reads the folder on every query.
    duckdb.connect().execute(f"COPY (SELECT 3 AS id, 30.0 AS amount) TO '{folder}/c.parquet'")
    assert execute_query(conn, "SELECT sum(amount) FROM orders")["rows"] == [[60.0]]
    blocked = execute_query(conn, f"SELECT * FROM read_csv('{secret}')")
    assert not blocked["success"] and blocked["error_type"] == "PermissionException"


def test_a_missing_path_fails_when_the_view_is_made(tmp_path):
    with pytest.raises(duckdb.Error):
        attach_data(duckdb.connect(), f"{tmp_path}/nothing/*.parquet")
