-- [Real Start Point Detection - Logic 2: Stable Phase Lowest Temperature]
-- Matches logic in tools/mark_real_start.py (Stable Phase + Fallback)

WITH base_data AS (
    SELECT
        all_metrics."timestamp",
        all_metrics.billet_cycle_id,
        all_metrics.temperature,
        all_metrics.current_speed,
        all_metrics.main_pressure,
        ROW_NUMBER() OVER (
            PARTITION BY all_metrics.billet_cycle_id
            ORDER BY all_metrics."timestamp" ASC
        ) as rn
    FROM
        public.all_metrics
    WHERE
        $__timeFilter(timestamp)
        AND all_metrics.billet_cycle_id IS NOT NULL
),
stable_phase_stats AS (
    SELECT
        base_data.billet_cycle_id,
        base_data.rn,
        base_data."timestamp",
        base_data.temperature,
        base_data.current_speed,
        base_data.main_pressure,
        
        -- [Past 30s Check] Min Speed
        MIN(base_data.current_speed) OVER (
            PARTITION BY base_data.billet_cycle_id
            ORDER BY base_data."timestamp" 
            ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
        ) as min_past_start_speed,
        
        -- [Future 10s Check] Speed Stats
        AVG(base_data.current_speed) OVER (
            PARTITION BY base_data.billet_cycle_id
            ORDER BY base_data."timestamp" 
            ROWS BETWEEN CURRENT ROW AND 10 FOLLOWING
        ) as fut_avg,
        MAX(base_data.current_speed) OVER (
            PARTITION BY base_data.billet_cycle_id
            ORDER BY base_data."timestamp" 
            ROWS BETWEEN CURRENT ROW AND 10 FOLLOWING
        ) as fut_max,
        MIN(base_data.current_speed) OVER (
            PARTITION BY base_data.billet_cycle_id
            ORDER BY base_data."timestamp" 
            ROWS BETWEEN CURRENT ROW AND 10 FOLLOWING
        ) as fut_min
    FROM
        base_data
),
stable_phase_candidates AS (
    SELECT
        *,
        -- Priority Ranking (Fallback Logic)
        -- 1. Strict Match: Has valid rest phase (< 1.0)
        -- 2. Fallback Match: No rest phase but stable
        CASE 
            WHEN min_past_start_speed < 1.0 THEN 1 
            ELSE 2 
        END as priority_rank
    FROM
        stable_phase_stats
    WHERE
        current_speed > 1.0
        AND fut_avg > 0
        AND (fut_max - fut_min) / NULLIF(fut_avg, 0) <= 0.05
),
stable_point_determined AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY billet_cycle_id
            ORDER BY priority_rank ASC, temperature ASC, rn ASC
        ) as final_rank
    FROM
        stable_phase_candidates
)

SELECT
    timestamp AS "time",
    billet_cycle_id,
    temperature,
    main_pressure,
    1 as "Real_Start_Point",
    priority_rank as "Debug_Priority (1=Strict, 2=Fallback)"
FROM
    stable_point_determined
WHERE
    final_rank = 1
ORDER BY
    timestamp ASC;