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
        
        -- Rank rows by Temperature (Lowest first) within each cycle
        ROW_NUMBER() OVER (
            PARTITION BY billet_cycle_id 
            ORDER BY temperature ASC, "timestamp" ASC
        ) as rn
    FROM
        base_with_rank
    WHERE
        time_rn <= 300 -- [Constraint] Limit search to first 300 rows (approx 1 min)
),
selected_min_points AS (
    SELECT
        *
    FROM
        cycle_min_candidates
    WHERE
        rn = 1 -- Select the single lowest point
)

SELECT
    timestamp as "time",
    billet_cycle_id,
    temperature,
    main_pressure,
    1 as "Real_Start_Point(Min_Temp)"
FROM
    selected_min_points
WHERE
    temperature < 530 -- [Threshold] Validity Filter (User Code: valid_points < 530)
ORDER BY
    timestamp ASC;
