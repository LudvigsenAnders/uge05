from src.rbac.rbac_env_config import EnvConfig


def test_env_overrides(monkeypatch):
    monkeypatch.setenv("POSTGRES_USER", "u1")
    monkeypatch.setenv("POSTGRES_PASSWORD", "p1")
    monkeypatch.setenv("POSTGRES_HOST", "h1")
    monkeypatch.setenv("POSTGRES_DB", "d1")

    cfg = EnvConfig()
    assert "u1" in cfg.db_admin_url
    assert "p1" in cfg.db_admin_url
    assert "h1" in cfg.db_admin_url
    assert "d1" in cfg.db_admin_url
