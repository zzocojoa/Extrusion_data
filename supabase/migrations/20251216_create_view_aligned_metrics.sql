-- View: view_aligned_metrics
-- Description: Aligns temperature data to pressure/speed start time by calculating dynamic row offsets per cycle.

CREATE OR REPLACE VIEW public.view_aligned_metrics AS
WITH base_data AS (
    SELECT
        *,
        -- Assign a unique row number within each cycle ordered by time
        ROW_NUMBER() OVER (PARTITION BY billet_cycle_id ORDER BY timestamp ASC) as rn
    FROM
        public.all_metrics
    WHERE
        billet_cycle_id IS NOT NULL
),

-- [Logic 1] Find Real Start Point (Zero Speed Point)
real_start_candidates AS (
    SELECT
        billet_cycle_id,
        rn,
        timestamp,
        current_speed,
        -- Look back 600 rows (approx 60s at 0.1s interval, or just rely on time)
        -- Assuming 1 row per record. Logic says "ROWS BETWEEN 60 PRECEDING".
        MIN(current_speed) OVER (PARTITION BY billet_cycle_id ORDER BY timestamp ROWS BETWEEN 60 PRECEDING AND 1 PRECEDING) as min_past_speed,
        -- Find last zero point in recent history
        MAX(CASE WHEN current_speed < 0.05 THEN rn END) OVER (PARTITION BY billet_cycle_id ORDER BY timestamp ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) as last_zero_rn
    FROM
        base_data
),
real_start_determined AS (
    SELECT
        billet_cycle_id,
        -- We take the first valid trigger point's detected zero point
        MIN(COALESCE(last_zero_rn, rn)) as start_rn
    FROM
        real_start_candidates
    WHERE
        current_speed >= 0.1
        AND min_past_speed < 0.8
    GROUP BY
        billet_cycle_id
),

-- [Logic 2] Find Stable Start Phase (Lowest Temp in Stable Zone)
stable_phase_stats AS (
    SELECT
        billet_cycle_id,
        rn,
        timestamp,
        temperature,
        current_speed,
        -- Context check: past 60 rows
        MIN(current_speed) OVER (PARTITION BY billet_cycle_id ORDER BY timestamp ROWS BETWEEN 60 PRECEDING AND CURRENT ROW) as past_min_speed,
        -- Future stability check: next 10 rows
        AVG(current_speed) OVER (PARTITION BY billet_cycle_id ORDER BY timestamp ROWS BETWEEN CURRENT ROW AND 10 FOLLOWING) as fut_avg,
        MAX(current_speed) OVER (PARTITION BY billet_cycle_id ORDER BY timestamp ROWS BETWEEN CURRENT ROW AND 10 FOLLOWING) as fut_max,
        MIN(current_speed) OVER (PARTITION BY billet_cycle_id ORDER BY timestamp ROWS BETWEEN CURRENT ROW AND 10 FOLLOWING) as fut_min
    FROM
        base_data
),
stable_phase_candidates AS (
    SELECT
        *,
        -- Rank candidates by temperature (lowest first)
        ROW_NUMBER() OVER (PARTITION BY billet_cycle_id ORDER BY temperature ASC, timestamp ASC) as temp_rank
    FROM
        stable_phase_stats
    WHERE
        current_speed > 1.0
        AND past_min_speed < 1.0
        AND fut_avg > 0
        AND (fut_max - fut_min) / NULLIF(fut_avg, 0) <= 0.05
),
stable_point_determined AS (
    SELECT
        billet_cycle_id,
        rn as stable_rn
    FROM
        stable_phase_candidates
    WHERE
        temp_rank = 1
),

-- [Calculate Offset]
cycle_offsets AS (
    SELECT
        rs.billet_cycle_id,
        rs.start_rn,
        sp.stable_rn,
        (sp.stable_rn - rs.start_rn) as offset_rows
    FROM
        real_start_determined rs
    JOIN
        stable_point_determined sp ON rs.billet_cycle_id = sp.billet_cycle_id
)

-- [Final View] Shift Temperature
SELECT
    t1.timestamp,
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
    
    -- Original Temperature (optional)
    t1.temperature as original_temperature,
    
    -- Shifted Temperature
    -- If offset exists, we pull temp from (rn + offset). 
    -- If no offset (conditions not met), keep original? Or NULL? 
    -- User request implies aligning. If not aligned, maybe just return original implies 0 shift.
    -- COALESCE(t2.temperature, t1.temperature) would keep original if shift fails or out of bounds.
    COALESCE(t2.temperature, t1.temperature) as temperature,
    
    -- Metadata for verification
    COALESCE(o.offset_rows, 0) as _debug_offset_rows
    
FROM
    base_data t1
LEFT JOIN
    cycle_offsets o ON t1.billet_cycle_id = o.billet_cycle_id
LEFT JOIN
    base_data t2 ON t1.billet_cycle_id = t2.billet_cycle_id AND t2.rn = (t1.rn + COALESCE(o.offset_rows, 0));
