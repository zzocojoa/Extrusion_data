-- Migration: Enable auto-refresh for view_optimized_aligned_metrics
-- Date: 2025-12-28
-- Description: Schedule 10-minute refresh via pg_cron (idempotent).

CREATE EXTENSION IF NOT EXISTS pg_cron;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM cron.job
        WHERE jobname = 'refresh_view_aligned_metrics_opt'
    ) THEN
        PERFORM cron.schedule(
            'refresh_view_aligned_metrics_opt',
            '*/10 * * * *',
            'REFRESH MATERIALIZED VIEW CONCURRENTLY public.view_optimized_aligned_metrics'
        );
    END IF;
END $$;
