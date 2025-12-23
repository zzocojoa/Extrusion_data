-- [Cycle Start Signal]
-- Finds the very first row of each Billet_CycleID chronologically
-- Equivalent to Python: df.drop_duplicates(subset=['Billet_CycleID'], keep='first')

WITH ranked_data AS (
  SELECT
    timestamp,
    billet_cycle_id,
    current_speed,
    ROW_NUMBER() OVER (
      PARTITION BY billet_cycle_id 
      ORDER BY timestamp ASC
    ) as rn
  FROM
    public.all_metrics
  WHERE
    $__timeFilter(timestamp)
    AND billet_cycle_id IS NOT NULL
)

SELECT
  timestamp AS "time",
  billet_cycle_id,
  current_speed,
  1 AS "Cycle_Start_Signal"
FROM
  ranked_data
WHERE
  rn = 1
ORDER BY
  timestamp ASC;