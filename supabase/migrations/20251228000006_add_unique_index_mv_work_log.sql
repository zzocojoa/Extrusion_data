-- Migration: Add unique index for concurrent refresh on mv_optimized_metrics_work_log
-- Date: 2025-12-28

DROP INDEX IF EXISTS public.idx_mv_optimized_metrics_work_log_ts;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_optimized_metrics_work_log_ts
    ON public.mv_optimized_metrics_work_log ("timestamp");
