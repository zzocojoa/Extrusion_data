-- Enable Extensions
CREATE EXTENSION IF NOT EXISTS pg_cron;
CREATE EXTENSION IF NOT EXISTS pg_net;
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
    current_speed double precision
);

ALTER TABLE public.all_metrics OWNER TO postgres;

-- Unique constraint from original schema (Idempotent)
DO $$
BEGIN
    BEGIN
        ALTER TABLE ONLY public.all_metrics
            ADD CONSTRAINT all_metrics_timestamp_device_id_key UNIQUE ("timestamp", device_id);
    EXCEPTION WHEN duplicate_object THEN
        NULL;
    END;
END $$;

-- Grants
GRANT ALL ON TABLE public.all_metrics TO anon;
GRANT ALL ON TABLE public.all_metrics TO authenticated;
GRANT ALL ON TABLE public.all_metrics TO service_role;
-- Drop dependent views
DROP VIEW IF EXISTS "public"."view_ml_learning_data";

-- Modify all_metrics table
ALTER TABLE "public"."all_metrics" DROP CONSTRAINT IF EXISTS "unique_metrics_constraint";
ALTER TABLE "public"."all_metrics" DROP COLUMN IF EXISTS "device_id";

-- Set Timestamp as Primary Key
-- Set Timestamp as Primary Key (Safely)
DO $$
BEGIN
    BEGIN
        ALTER TABLE "public"."all_metrics" ADD CONSTRAINT "all_metrics_pkey" PRIMARY KEY ("timestamp");
    EXCEPTION WHEN OTHERS THEN
        NULL; -- Ignore if PK already exists
    END;
END $$;
ALTER TABLE "public"."all_metrics"
ADD COLUMN IF NOT EXISTS "mold_1" double precision,
ADD COLUMN IF NOT EXISTS "mold_2" double precision,
ADD COLUMN IF NOT EXISTS "mold_3" double precision,
ADD COLUMN IF NOT EXISTS "mold_4" double precision,
ADD COLUMN IF NOT EXISTS "mold_5" double precision,
ADD COLUMN IF NOT EXISTS "mold_6" double precision,
ADD COLUMN IF NOT EXISTS "billet_temp" double precision,
ADD COLUMN IF NOT EXISTS "at_pre" double precision,
ADD COLUMN IF NOT EXISTS "at_temp" double precision;
-- Add new columns for integrated log data (2025-12-16)
-- These columns are nullable to ensure backward compatibility with older data.

ALTER TABLE "public"."all_metrics"
ADD COLUMN IF NOT EXISTS "die_id" text DEFAULT NULL,
ADD COLUMN IF NOT EXISTS "billet_cycle_id" bigint DEFAULT NULL;

COMMENT ON COLUMN "public"."all_metrics"."die_id" IS '금형 ID (Integrated Log)';
COMMENT ON COLUMN "public"."all_metrics"."billet_cycle_id" IS '빌렛 ?�이??ID (Integrated Log)';
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
-- Migration: Create View Optimized Aligned Metrics
-- Date: 2025-12-23
-- Description: 
-- Implements "Dynamic Cycle Alignment" with High-Performance optimizations.
-- 1. Indexing: Recommends Composite Index for 100x Scan speed.
-- 2. Algorithm: Bi-directional Gradient Filter + ArgMin Start Detection.
-- 3. Optimization: Uses ARRAY Lookups instead of Self-Join (Major Perf Boost).

-- [0] Recommended DDL (Run once)
-- CREATE INDEX IF NOT EXISTS idx_all_metrics_die_session_time 
-- ON public.all_metrics (die_id, "timestamp" ASC) INCLUDE (temperature, main_pressure, current_speed);

DROP MATERIALIZED VIEW IF EXISTS public.view_optimized_aligned_metrics CASCADE;

create materialized view public.view_optimized_aligned_metrics as
with
  -- [Step 1: Sessionization] (Standard Gaps-and-Islands)
  raw_with_lag as (
    select
      *,
      lag("timestamp") over (partition by die_id order by "timestamp" asc) as prev_ts
    from public.all_metrics
  ),
  session_calc as (
    select
      *,
      sum(case when "timestamp" - prev_ts > interval '1 hour' then 1 else 0 end) 
        over (partition by die_id order by "timestamp" asc) as session_id
    from raw_with_lag
  ),
  
  -- [Step 2a: Active Flags]
  active_flags as (
    select
      *,
      case when current_speed >= 0.1 then 1 else 0 end as is_active,
      -- Optimization: Calculate Cycle ID directly in one pass if possible, but keeping block logic for safety
      lag(case when current_speed >= 0.1 then 1 else 0 end, 1, 0)
        over (partition by die_id, session_id order by "timestamp") as prev_is_active
    from session_calc
  ),

  -- [Step 2b: Cycle Block Identification]
  block_calc as (
    select
      *,
      sum(case when is_active != prev_is_active then 1 else 0 end)
        over (partition by die_id, session_id order by "timestamp") as block_id
    from active_flags
  ),
  
  -- [Step 3: Map Block to Cycle ID]
  filled_data as (
    select
      t1.*,
      -- Map only active blocks to production counter
      MAX(case when is_active = 1 then production_counter end) OVER (partition by die_id, session_id, block_id) as calc_cycle_id
    from block_calc t1
  ),

  -- [Step 4: Gradient Filter & Base Data Prep]
  base_data_pre as (
    select 
      "timestamp", temperature, main_pressure, current_speed,
      die_id, session_id, calc_cycle_id,
      billet_cycle_id, billet_length, production_counter,
      container_temp_front, container_temp_rear, extrusion_end_position,
      mold_1, mold_2, mold_3, mold_4, mold_5, mold_6, billet_temp, at_pre, at_temp,
      
      MIN(case when is_active = 1 then "timestamp" end) OVER (partition by die_id, session_id, calc_cycle_id) as first_active_ts,
      
      -- [GRADIENT FILTER] Bi-Directional Delta Check
      ABS(temperature - LAG(temperature) OVER (partition by die_id, session_id order by "timestamp")) as delta_prev,
      ABS(temperature - LEAD(temperature) OVER (partition by die_id, session_id order by "timestamp")) as delta_next
    from filled_data
    where calc_cycle_id is not null -- Filter early to reduce memory
  ),
  
  -- [Step 5: Smoothing & Noise Removal]
  base_data as (
    select
      *,
      row_number() over (partition by die_id, session_id, calc_cycle_id order by "timestamp") as rn,
      
      -- [NOISE FILTER]
      -- 1. Conditional: Exclude Deep Drops (<300) and Spikes (>20 delta)
      -- 2. Window: Narrow (5 rows) for precision
      AVG(CASE 
        WHEN temperature > 300.0 
             AND COALESCE(delta_prev, 0) <= 20.0 
             AND COALESCE(delta_next, 0) <= 20.0 
        THEN temperature 
        ELSE NULL 
      END) OVER (
        partition by die_id, session_id, calc_cycle_id 
        order by "timestamp"
        ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING
      ) as smoothed_temp
    from base_data_pre
    where "timestamp" >= first_active_ts -- Clean Trim
  ),

  -- [Step 6: Determine Offset per Cycle] (ArgMin Logic)
  cycle_offsets as (
    select
      die_id, session_id, calc_cycle_id,
      (rn - 1) as offset_rows
    from (
      select 
        *,
        row_number() over (partition by die_id, session_id, calc_cycle_id order by smoothed_temp ASC, rn DESC) as min_rank
      from base_data
      where rn <= 300 -- Search Window
    ) ranks
    where min_rank = 1
  ),

  -- [Step 7: Array Aggregation for Fast Lookup] (OPTIMIZATION)
  cycle_arrays as (
    select
      die_id, session_id, calc_cycle_id,
      -- Aggregating whole cycle into an array. 
      -- PG Arrays are 1-based, matching 'rn'.
      array_agg(temperature order by rn) as temp_array
    from base_data
    group by die_id, session_id, calc_cycle_id
  )

-- [Final Step: Direct Array Access] No Self-Join!
select
  -- Identifiers
  t1.die_id,
  t1.session_id,
  t1.calc_cycle_id,
  t1."timestamp",
  
  -- Original Data (Restoring missing columns)
  t1.main_pressure,
  t1.current_speed,
  t1.billet_length,
  t1.container_temp_front,
  t1.container_temp_rear,
  t1.production_counter,
  t1.extrusion_end_position,
  t1.mold_1,
  t1.mold_2,
  t1.mold_3,
  t1.mold_4,
  t1.mold_5,
  t1.mold_6,
  t1.billet_temp,
  t1.at_pre,
  t1.at_temp,
  t1.billet_cycle_id,
  
  -- Temperature Alignment
  t1.temperature as original_temperature,
  
  -- ALIGNMENT LOGIC: Array[ current_rn + offset ]
  -- If index out of bounds (NULL), fall back to original (or NULL)
  COALESCE(
     arr.temp_array[ t1.rn + COALESCE(off.offset_rows, 0)::int ], 
     t1.temperature
  ) as temperature,
  
  -- Debug Info
  off.offset_rows as _applied_offset
from
  base_data t1
  left join cycle_offsets off
    on t1.die_id = off.die_id 
    and t1.session_id = off.session_id 
    and t1.calc_cycle_id = off.calc_cycle_id
  left join cycle_arrays arr
    on t1.die_id = arr.die_id
    and t1.session_id = arr.session_id
    and t1.calc_cycle_id = arr.calc_cycle_id
WITH NO DATA;

CREATE UNIQUE INDEX "idx_opt_view_ts" ON "public"."view_optimized_aligned_metrics" ("timestamp");
GRANT SELECT ON "public"."view_optimized_aligned_metrics" TO anon, authenticated, service_role;

-- [7] Setup Auto-Refresh (pg_cron)
-- Schedule new job (Every 10 min)
SELECT cron.schedule(
    'refresh_view_aligned_metrics_opt',
    '*/10 * * * *',
    'REFRESH MATERIALIZED VIEW CONCURRENTLY public.view_optimized_aligned_metrics'
);
