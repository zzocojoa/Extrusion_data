-- [Optimization] Ensure Source Indexes confirm existence for faster partitioning
CREATE INDEX IF NOT EXISTS "idx_all_metrics_die_ts" ON "public"."all_metrics" ("die_id", "timestamp");
CREATE INDEX IF NOT EXISTS "idx_all_metrics_billet_cycle" ON "public"."all_metrics" ("billet_cycle_id");

-- Drop the target view (Clean Slate)
DROP MATERIALIZED VIEW IF EXISTS public.view_aligned_metrics CASCADE;
DROP VIEW IF EXISTS public.view_aligned_metrics CASCADE;

create materialized view public.view_aligned_metrics as
with
  -- [Step 1: Raw Data with Sessionization per Die]
  -- Instead of splitting by Day (which breaks midnight cycles), 
  -- we split by "Session" within each Die.
  -- Session Breaks if: Time Gap > 1 Hour.
  
  raw_with_lag as (
    select
      *,
      -- Calculate Gap strictly within the same Die ID
      -- This allows us to use the index "idx_all_metrics_die_ts" efficiently
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
        -- Removed: "when date != date" check. We ALLOW crossing midnight now.
        else 0
      end) over (partition by die_id order by "timestamp" asc) as session_id
    from
      raw_with_lag
  ),
  
  -- [Step 2: Hybrid Block Logic (Identify Continuous Speed Blocks)]
  -- Python: df['is_active'] = df['현재속도'] >= 0.1
  block_calc_1 as (
    select
      *,
      -- [Parity] Revert to 0.1 to match Python exactly. 
      -- The 'Idle Head' filter downstream handles the noise.
      case when current_speed >= 0.1 then 1 else 0 end as is_active
    from
      session_calc
  ),
  
  -- Partition by DIE_ID and SESSION_ID ensures continuity
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
  
  -- [Step 3: Map Block to Cycle ID (Mode/Max/Min)]
  block_id_map as (
    select
      die_id,
      session_id,
      block_id,
      -- [Parity] STRICTLY use Production Counter, ignoring Billet_CycleID.
      MAX(production_counter) as mapped_cycle_id
    from
      block_calc_3
    where
      is_active = 1 -- Only look at active blocks for IDs
    group by
      die_id, session_id, block_id
  ),
  
  -- [Step 4: Assign & Fill ID]
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
  
  filled_data as (
    select
      *,
      -- Forward Fill the Mapped ID within the Session
      -- This naturally crosses midnight if the session is unbroken
      MAX(mapped_cycle_id) OVER (
        partition by die_id, session_id 
        order by "timestamp" asc 
        rows between unbounded preceding and current row
      ) as calc_cycle_id
    from
      assigned_data
  ),

  -- [Step 5: Base Data with Ranked Row Numbers]
  base_data_pre as (
    select 
      *,
      -- Calculate First Active Timestamp for each Cycle
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
      billet_cycle_id, -- Original
      calc_cycle_id,
      session_id,
      
      -- Create Row Number for the WHOLE extended cycle
      -- Partition by Die + Cycle ensures uniqueness per run
      row_number() over (
        partition by die_id, session_id, calc_cycle_id
        order by "timestamp"
      ) as rn
    from
      base_data_pre
    where
      calc_cycle_id is not null -- Filter early noise
      AND "timestamp" >= first_active_ts -- [Fix] Exclude Idle Head (Prevents 400k rows)
  ),
  
  -- [Step 6: Real Start Detection (Min + Tolerance)]
  cycle_min_candidates as (
    select
      die_id,
      session_id,
      calc_cycle_id,
      rn,
      temperature,
      MIN(temperature) OVER (
        PARTITION BY die_id, session_id, calc_cycle_id
      ) as min_temp_in_cycle
    from
      base_data
    where
      rn <= 300 -- [Constraint] First 300 rows (approx 1 min)
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
      temperature <= (min_temp_in_cycle + 2.0) -- Tolerance Filter
      and temperature < 530 -- Global Safety Filter
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
      time_rank = 1 -- Earliest point in Valley
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
  
  -- Apply Alignment
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
WITH NO DATA; -- [Optimization]

-- [Index Creation]
CREATE UNIQUE INDEX "idx_mat_view_timestamp" 
ON "public"."view_aligned_metrics" ("timestamp");

-- [Permissions]
GRANT SELECT ON "public"."view_aligned_metrics" TO anon, authenticated, service_role;

-- [Usage Note]
-- After running this script, you MUST run:
-- REFRESH MATERIALIZED VIEW public.view_aligned_metrics;
