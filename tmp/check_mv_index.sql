SELECT version, name
FROM supabase_migrations.schema_migrations
WHERE version = '20251228000006';

SELECT indexname, indexdef
FROM pg_indexes
WHERE schemaname = 'public'
  AND tablename = 'mv_optimized_metrics_work_log'
ORDER BY indexname;
