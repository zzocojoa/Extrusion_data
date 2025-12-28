-- Legacy Migration: 20251126235402_gui_update_20251127085339
-- REPLACED with Stub on 2025-12-25 to fix migration errors.
-- The objects created here (all_metrics_processed, metrics_view) are deprecated and removed in 20251225_cleanup_unused_objects.sql

DO $$ 
BEGIN
    -- Ensure clean state by dropping if exists
    BEGIN
        DROP VIEW IF EXISTS "public"."metrics_view" CASCADE;
        DROP MATERIALIZED VIEW IF EXISTS "public"."all_metrics_processed" CASCADE;
        DROP TABLE IF EXISTS "public"."all_metrics_processed" CASCADE;
    EXCEPTION WHEN OTHERS THEN
        NULL;
    END;
END $$;

-- Preserve Grants for persistent tables
GRANT SELECT ON TABLE "public"."all_metrics" TO "supabase_admin";
GRANT SELECT ON TABLE "public"."tb_work_log" TO "supabase_admin"; -- Retaining just in case, though table will be dropped later
