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
