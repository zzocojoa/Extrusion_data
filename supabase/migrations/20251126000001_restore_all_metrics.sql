-- Migration: Restore all_metrics table
-- Date: 2025-11-26 (Sequenced after baseline stub)

CREATE TABLE IF NOT EXISTS public.all_metrics (
    "timestamp" timestamp with time zone NOT NULL,
    device_id text NOT NULL,
    temperature double precision,
    main_pressure double precision,
    billet_length double precision,
    container_temp_front double precision,
    container_temp_rear double precision,
    production_counter bigint,
    current_speed double precision,
    extrusion_end_position double precision
);

ALTER TABLE public.all_metrics OWNER TO postgres;

-- Unique constraint from original schema (Idempotent)
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'all_metrics'
          AND column_name = 'device_id'
    ) THEN
        BEGIN
            ALTER TABLE ONLY public.all_metrics
                ADD CONSTRAINT all_metrics_timestamp_device_id_key UNIQUE ("timestamp", device_id);
        EXCEPTION WHEN duplicate_object THEN
            NULL;
        END;
    END IF;
END $$;

-- Grants
GRANT ALL ON TABLE public.all_metrics TO anon;
GRANT ALL ON TABLE public.all_metrics TO authenticated;
GRANT ALL ON TABLE public.all_metrics TO service_role;
