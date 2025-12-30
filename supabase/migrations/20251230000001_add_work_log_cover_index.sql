-- Migration: Add covering index for tb_work_log range matching
-- Date: 2025-12-30

CREATE INDEX IF NOT EXISTS idx_work_log_cover_perf
    ON public.tb_work_log (start_time, end_time, created_at DESC, id DESC)
    INCLUDE (die_id);
