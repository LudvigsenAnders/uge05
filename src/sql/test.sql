
-- =====================================================================
-- MULTI-SCHEMA ISOLATED ROLE SETUP
-- =====================================================================

-- ------------ 1. LOGIN ROLES (example) ------------
CREATE ROLE user_john LOGIN PASSWORD 'replace_me_john';
CREATE ROLE user_svc  LOGIN PASSWORD 'replace_me_service';

-- =====================================================================
-- 2. CONFIGURATION — LIST SCHEMAS HERE
-- =====================================================================
-- For each schema:
--   <schema>_admin
--   <schema>_rw
--   <schema>_ro
--
-- Example schemas: public, billing, analytics
-- Add/remove schemas as needed

DO $$
DECLARE
    schemas TEXT[] := ARRAY['public', 'billing', 'analytics'];
    s TEXT;
BEGIN

    FOREACH s IN ARRAY schemas LOOP

        RAISE NOTICE 'Configuring schema: %', s;

        ----------------------------------------------------------------
        -- 3. Create per-schema group roles
        ----------------------------------------------------------------
        EXECUTE format('CREATE ROLE %I_admin;', s);
        EXECUTE format('CREATE ROLE %I_rw;', s);
        EXECUTE format('CREATE ROLE %I_ro;', s);

        ----------------------------------------------------------------
        -- 4. Revoke insecure defaults
        ----------------------------------------------------------------
        EXECUTE format('REVOKE ALL ON SCHEMA %I FROM PUBLIC;', s);

        ----------------------------------------------------------------
        -- 5. Grant schema-level privileges
        ----------------------------------------------------------------

        -- Admin – full control
        EXECUTE format('GRANT ALL PRIVILEGES ON SCHEMA %I TO %I_admin;', s, s);
        EXECUTE format('GRANT ALL PRIVILEGES ON ALL TABLES    IN SCHEMA %I TO %I_admin;', s, s);
        EXECUTE format('GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA %I TO %I_admin;', s, s);

        -- Read/Write
        EXECUTE format('GRANT USAGE ON SCHEMA %I TO %I_rw;', s, s);
        EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA %I TO %I_rw;', s, s);
        EXECUTE format('GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA %I TO %I_rw;', s, s);

        -- Read-Only
        EXECUTE format('GRANT USAGE ON SCHEMA %I TO %I_ro;', s, s);
        EXECUTE format('GRANT SELECT ON ALL TABLES IN SCHEMA %I TO %I_ro;', s, s);
        EXECUTE format('GRANT SELECT ON ALL SEQUENCES IN SCHEMA %I TO %I_ro;', s, s);

        ----------------------------------------------------------------
        -- 6. Default privileges for future objects
        ----------------------------------------------------------------

        -- Admin
        EXECUTE format(
            'ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT ALL PRIVILEGES ON TABLES TO %I_admin;',
            s, s
        );
        EXECUTE format(
            'ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT ALL PRIVILEGES ON SEQUENCES TO %I_admin;',
            s, s
        );

        -- Read/Write
        EXECUTE format(
            'ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO %I_rw;',
            s, s
        );
        EXECUTE format(
            'ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT SELECT, UPDATE ON SEQUENCES TO %I_rw;',
            s, s
        );

        -- Read-Only
        EXECUTE format(
            'ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT SELECT ON TABLES TO %I_ro;',
            s, s
        );
        EXECUTE format(
            'ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT SELECT ON SEQUENCES TO %I_ro;',
            s, s
        );

        ----------------------------------------------------------------
        -- 7. Make admin role owner of the schema
        ----------------------------------------------------------------
        EXECUTE format('ALTER SCHEMA %I OWNER TO %I_admin;', s, s);

    END LOOP;
END$$;

-- =====================================================================
-- 8. DATABASE-LEVEL PERMISSIONS
-- =====================================================================
-- (optional but recommended)
REVOKE ALL ON DATABASE mydb FROM PUBLIC;
GRANT CONNECT ON DATABASE mydb TO user_john, user_svc;

-- Example: assign schema-level roles to login roles
GRANT public_rw     TO user_john;
GRANT billing_ro    TO user_john;
GRANT analytics_admin TO user_svc;

-- =====================================================================
-- END
-- =====================================================================
