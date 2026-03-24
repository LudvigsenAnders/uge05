from rbac.rbac_config import Config
from rbac.provisioner import RBACProvisioner
from rbac.audit import AuditLogger
from sqlalchemy import create_engine


def main():
    cfg = Config()

    audit_engine = create_engine(cfg.db_admin_url, future=True)
    audit_logger = AuditLogger(audit_engine, cfg.audit_log_file)

    p = RBACProvisioner(cfg, audit_logger)
    p.provision_all()


if __name__ == "__main__":
    main()
