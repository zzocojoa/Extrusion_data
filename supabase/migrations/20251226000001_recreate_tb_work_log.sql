-- Migration: Re-create tb_work_log
-- Date: 2025-12-26
-- Description: Re-creates the work log table with unified schema matching the Python uploader.

DROP TABLE IF EXISTS public.tb_work_log CASCADE;

CREATE TABLE public.tb_work_log (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    created_at timestamp with time zone DEFAULT now(),
    
    -- Key Identifiers
    machine_id text NOT NULL,
    start_time timestamp with time zone NOT NULL,
    end_time timestamp with time zone,
    
    -- Metadata
    worker_name text,
    die_id text,         -- DW No.
    alloy_type text,     -- 재질
    lot text,            -- LOT
    temper_type text,    -- 질별
    op_note text,        -- OP Note
    
    -- Process Metrics (Float/Numeric)
    target_billet_temp double precision, -- 온도
    target_exit_temp double precision,   -- 출구온도
    quenching_temp double precision,     -- 퀜칭온도
    stretching double precision,         -- 스트레칭
    total_weight double precision,       -- 중량
    ram double precision,                -- RAM
    actual_unit_weight double precision, -- 실단중
    yield_rate double precision,         -- 수율 (New)
    
    -- Production Counts (Integer)
    production_qty bigint,       -- 적합수량
    production_weight bigint,    -- 적합중량 (Rounded to int)
    productivity bigint,         -- 생산성 (Rounded to int)
    die_number bigint,           -- # (Number)
    product_length bigint,       -- 길이
    
    -- Defects (Integer)
    defect_bubble bigint DEFAULT 0,
    defect_tearing bigint DEFAULT 0,
    defect_white_black_line bigint DEFAULT 0,
    defect_oxide bigint DEFAULT 0,
    defect_scratch bigint DEFAULT 0,
    defect_bend bigint DEFAULT 0,
    defect_dimension bigint DEFAULT 0,
    defect_line bigint DEFAULT 0,
    defect_etc bigint DEFAULT 0,
    
    -- Cuts (Integer)
    start_cut bigint, -- S
    end_cut bigint    -- E
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_work_log_machine_time ON public.tb_work_log (machine_id, start_time DESC);
CREATE INDEX IF NOT EXISTS idx_work_log_die_id ON public.tb_work_log (die_id);

-- RLS
ALTER TABLE public.tb_work_log ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Enable read access for all users" ON public.tb_work_log
    FOR SELECT USING (true);

CREATE POLICY "Enable insert for authenticated users only" ON public.tb_work_log
    FOR INSERT WITH CHECK (auth.role() = 'authenticated' OR auth.role() = 'service_role');

CREATE POLICY "Enable update for authenticated users only" ON public.tb_work_log
    FOR UPDATE USING (auth.role() = 'authenticated' OR auth.role() = 'service_role');

CREATE POLICY "Enable delete for authenticated users only" ON public.tb_work_log
    FOR DELETE USING (auth.role() = 'authenticated' OR auth.role() = 'service_role');

-- Grants
GRANT ALL ON TABLE public.tb_work_log TO postgres;
GRANT ALL ON TABLE public.tb_work_log TO anon;
GRANT ALL ON TABLE public.tb_work_log TO authenticated;
GRANT ALL ON TABLE public.tb_work_log TO service_role;
