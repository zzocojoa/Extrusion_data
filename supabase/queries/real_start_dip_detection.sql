-- [Real Start Start Point Detection - Logic 3: Global Minimum per Cycle]
-- Implements the User's "Min Temp per Cycle" Python logic.
-- Logic:
--   1. Find the point with the Lowest Temperature for each Cycle.
--   2. Filter: Only accept if that Lowest Temperature is < 530 (Threshold as per request).

WITH base_with_rank AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY billet_cycle_id 
            ORDER BY "timestamp" ASC
        ) as time_rn
    FROM
        public.all_metrics
    WHERE
        billet_cycle_id IS NOT NULL
        AND $__timeFilter("timestamp")
),
cycle_min_candidates AS (
    SELECT
        billet_cycle_id,
        "timestamp",
        temperature,
        main_pressure,
        
        -- Calculate Min Temp for this cycle (limited to first 300 rows)
        MIN(temperature) OVER (
            PARTITION BY billet_cycle_id 
        ) as min_temp_in_cycle,
        
        -- Rank rows by TIME (Earliest first) to find start of valley
        ROW_NUMBER() OVER (
            PARTITION BY billet_cycle_id 
            ORDER BY "timestamp" ASC
        ) as time_rank_within_cycle
    FROM
        base_with_rank
    WHERE
        time_rn <= 300 -- [Constraint] Limit search to first 300 rows
),
selected_min_points AS (
    SELECT
        *
    FROM
        cycle_min_candidates
    WHERE
        temperature <= (min_temp_in_cycle + 2.0) -- [Tolerance] Within +2.0 of Min
    ORDER BY
        "timestamp" ASC
)
-- We need to pick the FIRST one per cycle from selected_min_points
, final_selection AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY billet_cycle_id 
            ORDER BY "timestamp" ASC
        ) as final_rn
    FROM 
        selected_min_points
)
SELECT 
    timestamp as "time",
    billet_cycle_id,
    temperature,
    main_pressure,
    1 as "Real_Start_Point(Min_Temp)"
FROM 
    final_selection
WHERE 
    final_rn = 1
