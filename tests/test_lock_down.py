"""lock_down: the agent's SQL reaches the attached data and nothing else on disk."""

import duckdb
import pytest

from duckdb_agent.sources import IN_MEMORY, connect, lock_down
from duckdb_agent.sql import execute_query


@pytest.fixture
def secret(tmp_path):
    path = tmp_path / "secret.csv"
    path.write_text("token\nhunter2\n")
    return str(path)


@pytest.fixture
def db_file(tmp_path):
    path = str(tmp_path / "data.duckdb")
    duckdb.connect(path).execute("CREATE TABLE t AS SELECT 1 AS a").close()
    return path


def assert_blocked(result):
    assert not result["success"]
    assert result["error_type"] == "PermissionException"


def test_file_source_can_query_its_data_but_not_other_files(db_file, secret):
    conn = connect(db_file, None)
    assert execute_query(conn, "SELECT a FROM t")["rows"] == [[1]]
    assert_blocked(execute_query(conn, f"SELECT * FROM read_csv('{secret}')"))
    assert_blocked(execute_query(conn, f"FROM '{secret}'"))


def test_loaded_files_are_read_before_the_lock(secret, tmp_path):
    data = tmp_path / "sales.csv"
    data.write_text("amount\n5\n")
    conn = connect(IN_MEMORY, None, load=[str(data)])
    assert execute_query(conn, "SELECT amount FROM sales")["rows"] == [[5]]
    assert_blocked(execute_query(conn, f"SELECT * FROM read_csv('{secret}')"))


def test_cursors_share_the_lock(db_file, secret):
    cursor = connect(db_file, None).cursor()
    cursor.execute("USE source")
    assert_blocked(execute_query(cursor, f"SELECT * FROM read_csv('{secret}')"))


def test_lock_cannot_be_undone(db_file):
    conn = connect(db_file, None)
    with pytest.raises(duckdb.Error, match="configuration has been locked"):
        conn.execute("SET enable_external_access = true")


def test_ducklake_data_path_stays_readable(tmp_path, secret):
    lake = tmp_path / "lake"
    lake.mkdir()
    writer = duckdb.connect()
    writer.execute(f"ATTACH 'ducklake:{lake}/meta.ducklake' AS dl (DATA_PATH '{lake}/')")
    writer.execute("CREATE TABLE dl.e AS SELECT range AS r FROM range(5)")
    writer.close()

    conn = connect(f"ducklake:{lake}/meta.ducklake", None)
    assert execute_query(conn, "SELECT count(*) FROM e")["rows"] == [[5]]
    assert_blocked(execute_query(conn, f"SELECT * FROM read_csv('{secret}')"))


def test_lock_down_on_a_callers_connection(db_file, secret):
    conn = duckdb.connect()
    conn.execute(f"ATTACH '{db_file}' AS source (READ_ONLY)")
    lock_down(conn)
    assert execute_query(conn, "SELECT a FROM source.t")["rows"] == [[1]]
    assert_blocked(execute_query(conn, f"SELECT * FROM read_csv('{secret}')"))


def test_saved_secrets_still_work_after_the_lock_down(tmp_path):
    """A DuckLake on S3 needs its saved S3 secret at the first data read, after the
    lock-down. DuckDB loads saved secrets from disk lazily, so they're loaded first; the
    secrets folder itself stays unreadable."""
    secrets = tmp_path / "stored_secrets"
    setup = duckdb.connect(config={"secret_directory": str(secrets)})
    setup.execute("CREATE PERSISTENT SECRET s3_bucket (TYPE s3, KEY_ID 'AKIAFAKE', SECRET 'fake', SCOPE 's3://probe-bucket')")
    setup.close()

    conn = duckdb.connect(config={"secret_directory": str(secrets)})
    lock_down(conn)
    found = conn.execute("FROM which_secret('s3://probe-bucket/data.parquet', 's3')").fetchall()
    assert found == [("s3_bucket", "PERSISTENT", "local_file")]
    assert found == conn.cursor().execute("FROM which_secret('s3://probe-bucket/data.parquet', 's3')").fetchall()
    assert_blocked(execute_query(conn, f"SELECT * FROM glob('{secrets}/*')"))


def test_https_can_read_the_ca_certificates_and_nothing_next_to_them():
    """HTTPS to S3 reads the system's CA bundle after the lock-down: that one file is
    allowed (public certificates), not its folder."""
    from duckdb_agent.sources import ca_bundle
    bundle = ca_bundle()
    if bundle is None:
        pytest.skip("no CA bundle on this machine")
    conn = duckdb.connect()
    lock_down(conn)
    assert conn.execute("SELECT current_setting('allowed_paths')").fetchone()[0] == [bundle]
    assert execute_query(conn, f"SELECT length(content) > 0 FROM read_text('{bundle}')")["rows"][0][0] is True
    assert_blocked(execute_query(conn, "SELECT * FROM read_text('/etc/hosts')"))
