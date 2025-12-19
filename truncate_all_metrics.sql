-- [CAUTION] This script deletes ALL data from the metrics tables.
-- Use this only if you want to re-upload everything from scratch.

-- 1. Clear the Main Data Table
TRUNCATE TABLE public.all_metrics;

-- 2. Clear the derived KST Table (if it exists)
TRUNCATE TABLE public.all_metrics_kst;

-- 3. Refresh the View (It will become empty)
REFRESH MATERIALIZED VIEW public.view_aligned_metrics;

-- Confirmation
SELECT 'All data has been cleared.' as status;
