from duckdb_agent import init_traces_db, reset_traces_db


def test_reset_deletes_database_and_wal(tmp_path):
    path = str(tmp_path / "traces.duckdb")
    conn = init_traces_db(path)
    conn.execute("INSERT INTO agent_runs (run_id, prompt) VALUES ('r1', 'q')")
    conn.close()
    (tmp_path / "traces.duckdb.wal").write_text("")

    assert reset_traces_db(path) is True
    assert not (tmp_path / "traces.duckdb").exists()
    assert not (tmp_path / "traces.duckdb.wal").exists()

    fresh = init_traces_db(path)
    assert fresh.execute("SELECT count(*) FROM agent_runs").fetchone()[0] == 0


def test_reset_missing_database_is_noop(tmp_path):
    assert reset_traces_db(str(tmp_path / "nope.duckdb")) is False
