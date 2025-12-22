-- Migration: Create View Aligned Metrics V2 (Strict Alignment)
-- Date: 2025-12-22
-- Description: 
-- Creates 'view_aligned_metrics_v2' which implements "Strict Cycle Alignment".
-- Difference from v1:
-- 1. Does NOT forward-fill Billet_CycleID into Null/Cooling gaps.
-- 2. Excludes Null Cycle rows entirely (Discontinuous Graph).
-- 3. Left Shift applies strictly to the active cycle duration.

-- [1] Clean Previous V2 (if exists)
DROP MATERIALIZED VIEW IF EXISTS public.view_aligned_metrics_v2 CASCADE;

-- [2] Create Materialized View V2
create materialized view public.view_aligned_metrics_v2 as
with
  -- [Step 1: Sessionization per Die] (Same as v1)
  raw_with_lag as (
    select
      *,
      lag("timestamp") over (partition by die_id order by "timestamp" asc) as prev_ts
    from
      public.all_metrics
  ),

  session_calc as (
    select
      *,
      sum(case 
        when prev_ts is null then 0
        when "timestamp" - prev_ts > interval '1 hour' then 1
        else 0
      end) over (partition by die_id order by "timestamp" asc) as session_id
    from
      raw_with_lag
  ),
  
  -- [Step 2: Hybrid Block Logic] (Same as v1)
  block_calc_1 as (
    select
      *,
      case when current_speed >= 0.1 then 1 else 0 end as is_active
    from
      session_calc
  ),
  
  block_calc_2 as (
    select
      *,
      lag(is_active, 1, 0) over (partition by die_id, session_id order by "timestamp" asc) as prev_active
    from
      block_calc_1
  ),
  
  block_calc_3 as (
    select
      *,
      sum(case when is_active != prev_active then 1 else 0 end) 
        over (partition by die_id, session_id order by "timestamp" asc) as block_id
    from
      block_calc_2
  ),
  
  -- [Step 3: Map Block to Cycle ID]
  block_id_map as (
    select
      die_id,
      session_id,
      block_id,
      MAX(production_counter) as mapped_cycle_id
    from
      block_calc_3
    where
      is_active = 1
    group by
      die_id, session_id, block_id
  ),
  
  -- [Step 4: Assign ID (STRICT MODE)]
  assigned_data as (
    select
      t1.*,
      map.mapped_cycle_id
    from
      block_calc_3 t1
      left join block_id_map map 
        on t1.die_id = map.die_id
        and t1.session_id = map.session_id
        and t1.block_id = map.block_id
  ),
  
  -- [Step 4b: No Forward Fill]
  -- In V1, we used MAX() OVER() here to fill nulls.
  -- In V2, we SKIP this. calc_cycle_id IS mapped_cycle_id.
  filled_data as (
    select
      *,
      mapped_cycle_id as calc_cycle_id
    from
      assigned_data
  ),

  -- [Step 5: Base Data]
  base_data_pre as (
    select 
      *,
      MIN(case when is_active = 1 then "timestamp" end) OVER (
        partition by die_id, session_id, calc_cycle_id
      ) as first_active_ts
    from 
      filled_data
  ),
  
  base_data as (
    select
      "timestamp",
      temperature,
      main_pressure,
      billet_length,
      container_temp_front,
      container_temp_rear,
      production_counter,
      current_speed,
      extrusion_end_position,
      mold_1,
      mold_2,
      mold_3,
      mold_4,
      mold_5,
      mold_6,
      billet_temp,
      at_pre,
      at_temp,
      die_id,
      billet_cycle_id,
      calc_cycle_id,
      session_id,
      
      -- Row Number for the STRICT cycle
      row_number() over (
        partition by die_id, session_id, calc_cycle_id
        order by "timestamp"
      ) as rn,
      
      -- [NOISE FILTER] 20-Point Moving Average
      -- Centered Window: 10 Preceding + Current + 9 Following = 20 Rows
      AVG(temperature) OVER (
        partition by die_id, session_id, calc_cycle_id 
        order by "timestamp"
        ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING
      ) as smoothed_temp
      
    from
      base_data_pre
    where
      calc_cycle_id is not null -- [STRICT] This filters out ALL Null Tails!
      AND "timestamp" >= first_active_ts -- Exclude Idle Head
  ),
  
  -- [Step 6: Real Start Detection using SMOOTHED TEMP]
  cycle_min_candidates as (
    select
      die_id,
      session_id,
      calc_cycle_id,
      rn,
      temperature,
      smoothed_temp,
      -- Find Min of the SMOOTHED curve
      MIN(smoothed_temp) OVER (
        PARTITION BY die_id, session_id, calc_cycle_id
      ) as min_smoothed_temp
    from
      base_data
    where
      rn <= 170
  ),
  
  cycle_start_candidates as (
    select 
      die_id,
      session_id,
      calc_cycle_id,
      rn,
      temperature,
      row_number() over (
        partition by die_id, session_id, calc_cycle_id
        order by rn ASC
      ) as time_rank
    from
      cycle_min_candidates
    where
      -- Compare SMOOTHED Temp to SMOOTHED Min
      smoothed_temp <= (min_smoothed_temp + 2.0)
      and temperature < 530 -- Global Safety Filter (Raw temp check is still useful for sanity)
  ),
  
  valid_start_points as (
    select
      die_id,
      session_id,
      calc_cycle_id,
      rn as stable_rn
    from
      cycle_start_candidates
    where
      time_rank = 1
  ),
  
  cycle_offsets as (
    select
      sp.die_id,
      sp.session_id,
      sp.calc_cycle_id,
      sp.stable_rn - 1 as offset_rows
    from
      valid_start_points sp
  )

select
  t1."timestamp",
  t1.billet_cycle_id,
  t1.main_pressure,
  t1.billet_length,
  t1.container_temp_front,
  t1.container_temp_rear,
  t1.production_counter,
  t1.current_speed,
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
  t1.die_id,
  t1.temperature as original_temperature,
  
  -- Apply Alignment (Left Shift)
  -- Note: Since t2 has no tail, the end of the line will be NULL (or unshifted if logic kept)
  -- Here we keep COALESCE(t2, t1) which means the tail will show UN-SHIFTED data (original).
  -- If you want strictly NULL tail, remove COALESCE. Kept for safety for now.
  COALESCE(t2.temperature, t1.temperature) as temperature,
  
  COALESCE(o.offset_rows, 0::bigint) as _debug_offset_rows,
  t1.calc_cycle_id as _debug_calc_id,
  t1.rn as _debug_current_rn,
  t1.session_id as _debug_session_id
from
  base_data t1
  left join cycle_offsets o 
    on t1.calc_cycle_id = o.calc_cycle_id 
    and t1.die_id = o.die_id
    and t1.session_id = o.session_id
  left join base_data t2 
    on t1.calc_cycle_id = t2.calc_cycle_id 
    and t1.die_id = t2.die_id
    and t1.session_id = t2.session_id
    and t2.rn = (t1.rn + COALESCE(o.offset_rows, 0::bigint))
WITH NO DATA;

-- [5] Index & Permissions
CREATE UNIQUE INDEX "idx_mat_view_timestamp_v2" 
ON "public"."view_aligned_metrics_v2" ("timestamp");

GRANT SELECT ON "public"."view_aligned_metrics_v2" TO anon, authenticated, service_role;

-- [6] Setup Auto-Refresh for V2 (Separate Job)
-- Schedule new job (Every 10 min)
SELECT cron.schedule(
    'refresh_view_aligned_metrics_v2',
    '*/10 * * * *',
    'REFRESH MATERIALIZED VIEW CONCURRENTLY public.view_aligned_metrics_v2'
);
