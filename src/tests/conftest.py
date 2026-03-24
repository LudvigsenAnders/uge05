import pytest
from rbac.audit import AuditLogger
from rbac.provisioner import RBACProvisioner
from rbac.rbac_config import Config
from sqlalchemy.sql import text


#
# Realistic SQLAlchemy mock infrastructure
#
class FakeTransaction:
    def __init__(self, conn):
        self.conn = conn
        self.committed = False
        self.rolled_back = False

    def __enter__(self):
        return self.conn

    def __exit__(self, exc_type, exc, tb):
        return False

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True


class FakeConnection:
    def __init__(self):
        self.executed = []     # store (sql, params)
        self.begin_called = False
        self.transaction = FakeTransaction(self)

    def execute(self, sql, params=None):
        self.executed.append((sql, params or {}))

    def begin(self):
        self.begin_called = True
        return self.transaction

    # to support `with engine.connect() as conn:`
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class FakeEngine:
    def __init__(self):
        self.conn = FakeConnection()

    def connect(self):
        return self.conn

    def begin(self):  # rarely used by your code, but let's support it
        return FakeTransaction(self.conn)


#
# Fixtures
#
@pytest.fixture
def mock_engine():
    return FakeEngine()


@pytest.fixture
def audit_engine():
    return FakeEngine()


@pytest.fixture
def audit_logger(audit_engine, tmp_path):
    logfile = tmp_path / "audit.log"
    return AuditLogger(audit_engine, str(logfile))


@pytest.fixture
def config():
    return Config()


@pytest.fixture
def provisioner(config, audit_logger, mock_engine, monkeypatch):
    monkeypatch.setattr("rbac.provisioner.create_engine",
                        lambda url, **kw: mock_engine)
    return RBACProvisioner(config, audit_logger)
