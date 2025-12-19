-- Drop the existing view/materialized view
DROP MATERIALIZED VIEW IF EXISTS public.view_aligned_metrics CASCADE;
DROP VIEW IF EXISTS public.view_aligned_metrics CASCADE;

create materialized view public.view_aligned_metrics as
with
  -- [Step 1: Sessionization (Time Gap Detection)]
  -- Use LAG simply to detect massive time jumps (e.g. > 12 hours) which imply a different production run.
  -- This protects the MAX() logic from carrying over IDs across years/resets.
  raw_with_lag as (
    select
      *,
      lag("timestamp") over (order by "timestamp" asc) as prev_ts
    from
      public.all_metrics
  ),

  session_calc as (
    select
      *,
      sum(case 
        when prev_ts is null then 0
        when "timestamp" - prev_ts > interval '12 hours' then 1
        else 0
      end) over (order by "timestamp" asc) as session_id
    from
      raw_with_lag
  ),
  
  -- [Step 2: ID Filling (Handle Nulls & Noise)]
  -- Logic: Within a session, Billet_CycleID should be increasing. 
  -- We use MAX() over unbounded preceding to Forward Fill the ID into Nulls and ignore 0-noise (if 0 < current max).
  -- This creates 'calc_cycle_id' which groups the Null Tail with the preceding Cycle.
  filled_data as (
    select
      *,
      MAX(billet_cycle_id) over (
        partition by session_id 
        order by "timestamp" asc 
        rows between unbounded preceding and current row
      ) as calc_cycle_id
    from
      session_calc
  ),

  -- [Step 3: Base Data with Ranked Row Numbers]
  -- Now we group by 'calc_cycle_id'.
  base_data as (
    select
      -- Select all columns needed
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
      billet_cycle_id, -- Keep original (with Nulls)
      session_id,
      calc_cycle_id,   -- Used for grouping
      
      -- Create Row Number for the WHOLE extended cycle (including tail)
      row_number() over (
        partition by session_id, calc_cycle_id
        order by "timestamp"
      ) as rn
    from
      filled_data
    where
      calc_cycle_id is not null -- Filter out leading noise before first ID
  ),
  
  -- [Step 4: Min Temp Detection within Extended Group]
  -- Logic 3: Min Temp < 530. We search the WHOLE extended group.
  -- [FIX] Limit search to first 300 rows (approx 1 min) to avoid finding Min Temp in the cooling tail.
  cycle_min_candidates as (
    select
      session_id,
      calc_cycle_id,
      rn,
      temperature,
      row_number() over (
        partition by session_id, calc_cycle_id
        order by temperature ASC, "timestamp" ASC
      ) as temp_rank
    from
      base_data
    where
      rn <= 300 -- [Constraint] First 300 rows only
  ),
  
  valid_start_points as (
    select
      session_id,
      calc_cycle_id,
      rn as stable_rn
    from
      cycle_min_candidates
    where
      temp_rank = 1
      and temperature < 530 -- Threshold
  ),
  
  cycle_offsets as (
    select
      sp.session_id,
      sp.calc_cycle_id,
      sp.stable_rn - 1 as offset_rows
    from
      valid_start_points sp
  )

select
  t1."timestamp",
  t1.billet_cycle_id, -- Original ID (can be Null)
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
  
  -- Apply Alignment using Offset
  -- If offset exists, we pull data from typical 'LEAD' logic?
  -- Python: shift(-offset) -> Value at T comes from T+offset.
  -- SQL: LEAD(temp, offset).
  -- BUT we need to do this JOIN style or Window Function style.
  -- Join style (t2.rn = t1.rn + offset) is easier for creating the view logic.
  COALESCE(t2.temperature, t1.temperature) as temperature,
  
  COALESCE(o.offset_rows, 0::bigint) as _debug_offset_rows,
  t1.calc_cycle_id as _debug_calc_id -- Helpful for seeing how it grouped
from
  base_data t1
  left join cycle_offsets o 
    on t1.calc_cycle_id = o.calc_cycle_id 
    and t1.session_id = o.session_id
  left join base_data t2 
    on t1.calc_cycle_id = t2.calc_cycle_id 
    and t1.session_id = t2.session_id
    and t2.rn = (t1.rn + COALESCE(o.offset_rows, 0::bigint));

-- [Index Creation]
-- Note: 'billet_cycle_id' is not unique anymore (could be null). 
-- So unique index might need to be on (timestamp).
CREATE UNIQUE INDEX "idx_mat_view_timestamp" 
ON "public"."view_aligned_metrics" ("timestamp");

-- [Permissions]
GRANT SELECT ON "public"."view_aligned_metrics" TO anon, authenticated, service_role;
