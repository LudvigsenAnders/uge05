from rbac_env_config import EnvConfig, UserSecrets
from provisioner import RBACProvisioner
from audit import AuditLogger
from rbac_config_loader import RBACConfig
from pathlib import Path


def main():
    secrets = UserSecrets()
    cfg_path = Path("src/rbac/config")

    # Load environment + DB URL from .env
    env_cfg = EnvConfig()
    # Load JSON RBAC model
    rbac_cfg = RBACConfig(cfg_path, secrets)

    print("Database:", rbac_cfg.database)
    print("Schemas:", list(rbac_cfg.schemas.keys()))
    print("Login roles:", rbac_cfg.get_all_login_roles())

    audit_logger = AuditLogger(env_cfg, env_cfg.audit_log_file)

    provisioner = RBACProvisioner(env_cfg, rbac_cfg, audit_logger)
    provisioner.provision_all()
    print("provision finished")


if __name__ == "__main__":
    main()
