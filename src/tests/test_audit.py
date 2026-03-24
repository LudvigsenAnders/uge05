
def test_audit_file_written(audit_logger, tmp_path):
    audit_logger.log("ACTION", "DETAIL")
    text = (tmp_path / "audit.log").read_text()
    assert "ACTION" in text
    assert "DETAIL" in text


def test_audit_db_written(audit_logger, audit_engine):
    audit_logger.log("ACTION", "DETAIL")

    conn = audit_engine.conn  # FakeConnection

    assert len(conn.executed) == 1
    sql, params = conn.executed[0]

    assert "INSERT INTO security_audit" in str(sql)
    assert params["action"] == "ACTION"
    assert params["detail"] == "DETAIL"
