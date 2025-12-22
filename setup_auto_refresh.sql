-- 1. Enable the extension (Must be run by Superuser/Service Role)
CREATE EXTENSION IF NOT EXISTS pg_cron;

-- 2. Schedule the job (Every 10 minutes)
-- Cron Syntax: '*/10 * * * *' means "Every 10th minute of every hour"
SELECT cron.schedule(
    'refresh_view_aligned_metrics', -- Job Name (Unique)
    '*/10 * * * *',                 -- Schedule
    'REFRESH MATERIALIZED VIEW CONCURRENTLY public.view_aligned_metrics' -- Command
);

-- [Verification]
-- Check if the job is scheduled
SELECT * FROM cron.job;

-- [Cleanup / Stop]
-- If you want to stop it later, uncomment and run:
-- SELECT cron.unschedule('refresh_view_aligned_metrics');
