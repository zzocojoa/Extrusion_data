-- Migration: Enable auto-refresh for work log MVs (split jobs)
-- Date: 2025-12-30
-- Description: Schedule 10-minute refresh via pg_cron (idempotent).

CREATE EXTENSION IF NOT EXISTS pg_cron;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM cron.job
        WHERE jobname = 'refresh_mv_work_log_ranges_10m'
    ) THEN
        PERFORM cron.schedule(
            'refresh_mv_work_log_ranges_10m',
            '*/10 * * * *',
            'REFRESH MATERIALIZED VIEW public.mv_work_log_effective_ranges'
        );
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM cron.job
        WHERE jobname = 'refresh_mv_work_log_10m_concurrent'
    ) THEN
        PERFORM cron.schedule(
            'refresh_mv_work_log_10m_concurrent',
            '2-59/10 * * * *',
            'REFRESH MATERIALIZED VIEW CONCURRENTLY public.mv_optimized_metrics_work_log'
        );
    END IF;
END $$;
