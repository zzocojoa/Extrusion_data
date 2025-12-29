-- Migration: Improve tb_work_log for shift/date, source tracking, and data quality
-- Date: 2025-12-28

-- 1) Add columns (safe, idempotent)
ALTER TABLE public.tb_work_log
    ADD COLUMN IF NOT EXISTS work_date date,
    ADD COLUMN IF NOT EXISTS shift_type text,
    ADD COLUMN IF NOT EXISTS source_file text,
    ADD COLUMN IF NOT EXISTS source_row integer,
    ADD COLUMN IF NOT EXISTS upload_batch_id text,
    ADD COLUMN IF NOT EXISTS data_quality_flags jsonb;

-- 2) Derive work_date / shift_type from start_time (KST)
CREATE OR REPLACE FUNCTION public.set_tb_work_log_work_date_shift()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    local_ts timestamp;
BEGIN
    IF NEW.start_time IS NULL THEN
        NEW.work_date := NULL;
        NEW.shift_type := NULL;
        RETURN NEW;
    END IF;

    local_ts := NEW.start_time AT TIME ZONE 'Asia/Seoul';
    NEW.work_date := local_ts::date;
    IF local_ts::time >= time '08:00' AND local_ts::time < time '20:00' THEN
        NEW.shift_type := 'day';
    ELSE
        NEW.shift_type := 'night';
    END IF;
    RETURN NEW;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_trigger t
        JOIN pg_class c ON c.oid = t.tgrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE t.tgname = 'trg_tb_work_log_set_work_date_shift'
          AND n.nspname = 'public'
          AND c.relname = 'tb_work_log'
    ) THEN
        CREATE TRIGGER trg_tb_work_log_set_work_date_shift
        BEFORE INSERT OR UPDATE OF start_time
        ON public.tb_work_log
        FOR EACH ROW
        EXECUTE FUNCTION public.set_tb_work_log_work_date_shift();
    END IF;
END $$;

-- Backfill existing rows
UPDATE public.tb_work_log
SET
    work_date = (start_time AT TIME ZONE 'Asia/Seoul')::date,
    shift_type = CASE
        WHEN (start_time AT TIME ZONE 'Asia/Seoul')::time >= time '08:00'
         AND (start_time AT TIME ZONE 'Asia/Seoul')::time < time '20:00'
        THEN 'day'
        ELSE 'night'
    END
WHERE work_date IS NULL OR shift_type IS NULL;

-- 3) Indexes for reporting
CREATE INDEX IF NOT EXISTS idx_work_log_work_date
    ON public.tb_work_log (work_date);

CREATE INDEX IF NOT EXISTS idx_work_log_work_date_die
    ON public.tb_work_log (work_date, die_id);

CREATE INDEX IF NOT EXISTS idx_work_log_shift_type
    ON public.tb_work_log (shift_type);

-- 4) Source tracking dedupe (safe: only enforces when source fields are set)
CREATE UNIQUE INDEX IF NOT EXISTS idx_work_log_source_row_unique
    ON public.tb_work_log (source_file, source_row)
    WHERE source_file IS NOT NULL AND source_row IS NOT NULL;

-- 5) Data quality constraints (NOT VALID to avoid blocking existing rows)
DO $$
BEGIN
    BEGIN
        ALTER TABLE public.tb_work_log
            ADD CONSTRAINT work_log_nonneg_production_qty
            CHECK (production_qty IS NULL OR production_qty >= 0) NOT VALID;
    EXCEPTION WHEN duplicate_object THEN
        NULL;
    END;

    BEGIN
        ALTER TABLE public.tb_work_log
            ADD CONSTRAINT work_log_nonneg_production_weight
            CHECK (production_weight IS NULL OR production_weight >= 0) NOT VALID;
    EXCEPTION WHEN duplicate_object THEN
        NULL;
    END;

    BEGIN
        ALTER TABLE public.tb_work_log
            ADD CONSTRAINT work_log_nonneg_productivity
            CHECK (productivity IS NULL OR productivity >= 0) NOT VALID;
    EXCEPTION WHEN duplicate_object THEN
        NULL;
    END;

    BEGIN
        ALTER TABLE public.tb_work_log
            ADD CONSTRAINT work_log_nonneg_total_weight
            CHECK (total_weight IS NULL OR total_weight >= 0) NOT VALID;
    EXCEPTION WHEN duplicate_object THEN
        NULL;
    END;

    BEGIN
        ALTER TABLE public.tb_work_log
            ADD CONSTRAINT work_log_nonneg_ram
            CHECK (ram IS NULL OR ram >= 0) NOT VALID;
    EXCEPTION WHEN duplicate_object THEN
        NULL;
    END;

    BEGIN
        ALTER TABLE public.tb_work_log
            ADD CONSTRAINT work_log_nonneg_product_length
            CHECK (product_length IS NULL OR product_length >= 0) NOT VALID;
    EXCEPTION WHEN duplicate_object THEN
        NULL;
    END;

    BEGIN
        ALTER TABLE public.tb_work_log
            ADD CONSTRAINT work_log_nonneg_actual_unit_weight
            CHECK (actual_unit_weight IS NULL OR actual_unit_weight >= 0) NOT VALID;
    EXCEPTION WHEN duplicate_object THEN
        NULL;
    END;

    BEGIN
        ALTER TABLE public.tb_work_log
            ADD CONSTRAINT work_log_nonneg_yield_rate
            CHECK (yield_rate IS NULL OR yield_rate >= 0) NOT VALID;
    EXCEPTION WHEN duplicate_object THEN
        NULL;
    END;

    BEGIN
        ALTER TABLE public.tb_work_log
            ADD CONSTRAINT work_log_nonneg_cut_counts
            CHECK (
                (start_cut IS NULL OR start_cut >= 0) AND
                (end_cut IS NULL OR end_cut >= 0)
            ) NOT VALID;
    EXCEPTION WHEN duplicate_object THEN
        NULL;
    END;

    BEGIN
        ALTER TABLE public.tb_work_log
            ADD CONSTRAINT work_log_nonneg_die_number
            CHECK (die_number IS NULL OR die_number >= 0) NOT VALID;
    EXCEPTION WHEN duplicate_object THEN
        NULL;
    END;

    BEGIN
        ALTER TABLE public.tb_work_log
            ADD CONSTRAINT work_log_nonneg_defects
            CHECK (
                (defect_bubble IS NULL OR defect_bubble >= 0) AND
                (defect_tearing IS NULL OR defect_tearing >= 0) AND
                (defect_white_black_line IS NULL OR defect_white_black_line >= 0) AND
                (defect_oxide IS NULL OR defect_oxide >= 0) AND
                (defect_scratch IS NULL OR defect_scratch >= 0) AND
                (defect_bend IS NULL OR defect_bend >= 0) AND
                (defect_dimension IS NULL OR defect_dimension >= 0) AND
                (defect_line IS NULL OR defect_line >= 0) AND
                (defect_etc IS NULL OR defect_etc >= 0)
            ) NOT VALID;
    EXCEPTION WHEN duplicate_object THEN
        NULL;
    END;
END $$;
