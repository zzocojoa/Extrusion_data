-- Migration: Drop product_name from tb_work_log
-- Reason: Column is unused and source mapping is intentionally removed.

ALTER TABLE "public"."tb_work_log"
    DROP COLUMN IF EXISTS "product_name";
