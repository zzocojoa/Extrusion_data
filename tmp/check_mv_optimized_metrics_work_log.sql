SELECT matviewname, ispopulated
FROM pg_matviews
WHERE schemaname = 'public' AND matviewname = 'mv_optimized_metrics_work_log';

SELECT indexname, indexdef
FROM pg_indexes
WHERE schemaname = 'public' AND tablename = 'mv_optimized_metrics_work_log';
