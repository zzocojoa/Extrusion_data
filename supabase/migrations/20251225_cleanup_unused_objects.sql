-- Migration: Cleanup Unused Objects
-- Date: 2025-12-25
-- Description: 
-- Remove legacy views, materialized views, and unused tables.
-- Retains ONLY 'all_metrics' and 'view_optimized_aligned_metrics'.
-- Drops 'tb_work_log' to be re-created later.

-- 1. Drop Legacy Views first (due to dependencies)
DROP VIEW IF EXISTS "public"."metrics_view";

-- 2. Drop Legacy Materialized Views (CASCADE to remove indexes/dependents)
DROP MATERIALIZED VIEW IF EXISTS "public"."all_metrics_processed" CASCADE;
DROP MATERIALIZED VIEW IF EXISTS "public"."view_aligned_metrics" CASCADE;
DROP MATERIALIZED VIEW IF EXISTS "public"."view_aligned_metrics_v2" CASCADE;

-- 3. Drop Unused Tables (CASCADE to remove RLS policies/Grants)
DROP TABLE IF EXISTS "public"."cycle_log" CASCADE;
DROP TABLE IF EXISTS "public"."temp_machine_starts" CASCADE;
DROP TABLE IF EXISTS "public"."tb_work_log" CASCADE;

-- 4. Clean up any orphaned functions if necessary (Optional, but good practice)
-- (None explicitly identified as purely orphan, keeping shared helpers)
