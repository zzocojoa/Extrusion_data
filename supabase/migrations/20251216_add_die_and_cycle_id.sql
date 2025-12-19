-- Add new columns for integrated log data (2025-12-16)
-- These columns are nullable to ensure backward compatibility with older data.

ALTER TABLE "public"."all_metrics"
ADD COLUMN IF NOT EXISTS "die_id" text DEFAULT NULL,
ADD COLUMN IF NOT EXISTS "billet_cycle_id" bigint DEFAULT NULL;

COMMENT ON COLUMN "public"."all_metrics"."die_id" IS '금형 ID (Integrated Log)';
COMMENT ON COLUMN "public"."all_metrics"."billet_cycle_id" IS '빌렛 사이클 ID (Integrated Log)';
