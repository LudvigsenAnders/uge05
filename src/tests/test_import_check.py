
def test_import_check():
    import rbac.provisioner
    import rbac.audit
    import rbac.masking
    import rbac.errors
    import src.rbac.rbac_env_config

    print("rbac.provisioner file:", rbac.provisioner.__file__)
    print("rbac.audit file:", rbac.audit.__file__)
    print("rbac.masking file:", rbac.masking.__file__)
    print("rbac.errors file:", rbac.errors.__file__)
    print("rbac.rbac_config file:", rbac.rbac_env_config.__file__)
