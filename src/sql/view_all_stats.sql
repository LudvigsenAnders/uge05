-- Show read statistics for all user tables
SELECT
    schemaname,
    relname AS table_name,
    seq_scan,       -- Number of sequential scans
    seq_tup_read,   -- Tuples read via sequential scans
    idx_scan,       -- Number of index scans
    idx_tup_fetch,  -- Tuples fetched via index scans
    n_tup_ins,      -- Tuples inserted
    n_tup_upd,      -- Tuples updated
    n_tup_del,      -- Tuples deleted
    last_vacuum,
    last_autovacuum,
    last_analyze,
    last_autoanalyze
FROM pg_stat_user_tables
ORDER BY (seq_scan + idx_scan) DESC;