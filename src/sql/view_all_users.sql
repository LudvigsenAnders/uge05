SELECT 
    u.usename AS role_name,
    CASE
        WHEN u.usesuper AND u.usecreatedb THEN 'superuser, create database'
        WHEN u.usesuper THEN 'superuser'
        WHEN u.usecreatedb THEN 'create database'
        ELSE ''
    END AS role_attributes,
    COALESCE(string_agg(r.rolname, ', '), '') AS member_of
FROM pg_catalog.pg_user u
LEFT JOIN pg_auth_members m
    ON m.member = u.usesysid
LEFT JOIN pg_roles r
    ON r.oid = m.roleid
GROUP BY u.usename, u.usesuper, u.usecreatedb
ORDER BY role_name DESC;