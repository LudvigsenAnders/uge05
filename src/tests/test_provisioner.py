
import pytest
from sqlalchemy.sql import text
from rbac.errors import SQLExecutionError


def test_run_executes_sql(provisioner, mock_engine):
    conn = mock_engine.conn

    provisioner.run(conn, "SELECT 1;", "Test Action")

    # FIRST: provisioning execute
    assert len(conn.executed) == 1
    sql, params = conn.executed[0]
    assert str(sql) == str(text("SELECT 1;"))
    assert params == {}

    # SECOND: audit execute
    audit_conn = provisioner.audit.audit_engine.conn
    assert len(audit_conn.executed) == 1


def test_run_logs_error_and_raises(provisioner, mock_engine):
    conn = mock_engine.conn

    # Force execute to fail
    def explode(*args, **kwargs):
        raise Exception("db error")

    conn.execute = explode

    with pytest.raises(SQLExecutionError):
        provisioner.run(conn, "SELECT 1;", "Test Action")

    audit_conn = provisioner.audit.audit_engine.conn
    assert len(audit_conn.executed) == 1  # error logged


def test_provision_all_commits(provisioner, mock_engine, monkeypatch):
    conn = mock_engine.conn
    trans = conn.transaction

    monkeypatch.setattr(provisioner, "create_login_roles", lambda c: None)
    monkeypatch.setattr(provisioner, "create_schema_roles", lambda c: None)
    monkeypatch.setattr(provisioner, "attach_logins", lambda c: None)

    provisioner.provision_all()

    assert trans.committed is True
    assert trans.rolled_back is False


def test_provision_all_rolls_back_on_error(provisioner, mock_engine, monkeypatch):
    conn = mock_engine.conn
    trans = conn.transaction

    monkeypatch.setattr(
        provisioner,
        "create_login_roles",
        lambda c: (_ for _ in ()).throw(Exception("boom"))
    )

    with pytest.raises(Exception):
        provisioner.provision_all()

    assert trans.rolled_back is True
    assert trans.committed is False
