-- Manual History Sync
CREATE SCHEMA IF NOT EXISTS supabase_migrations;
CREATE TABLE IF NOT EXISTS supabase_migrations.schema_migrations (
    version text PRIMARY KEY,
    name text,
    statements text[]
);

INSERT INTO supabase_migrations.schema_migrations (version, name, statements) VALUES
('20251126000000', 'baseline', NULL),
('20251126000001', 'restore_all_metrics', NULL),
('20251126075643', 'gui_update_20251126165619', NULL),
('20251126235402', 'gui_update_20251127085339', NULL),
('20251201', 'enable_rls_all_metrics', NULL),
('20251203', 'add_reload_func', NULL),
('20251203000002', 'create_cycle_log', NULL),
('20251209000001', 'unified_schema', NULL),
('20251209000002', 'add_integrated_columns', NULL),
('20251216', 'add_die_and_cycle_id', NULL),
('20251221', 'finalize_aligned_metrics_view', NULL),
('20251222', 'create_view_aligned_metrics_v2', NULL),
('20251223', 'view_optimized_aligned_metrics', NULL),
('20251224000001', 'add_work_log_columns', NULL),
('20251224000002', 'add_work_log_die_and_yield', NULL),
('20251224000003', 'grant_tb_work_log', NULL),
('20251225', 'cleanup_unused_objects', NULL)
ON CONFLICT (version) DO NOTHING;
