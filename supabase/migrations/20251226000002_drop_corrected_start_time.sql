-- Migration: Drop corrected_start_time from tb_work_log
-- Reason: Column is unused and the source file does not provide it.

ALTER TABLE "public"."tb_work_log"
    DROP COLUMN IF EXISTS "corrected_start_time";
