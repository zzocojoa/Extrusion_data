WITH wl AS (
  SELECT id, start_time, end_time
  FROM public.tb_work_log
  WHERE (start_time AT TIME ZONE 'Asia/Seoul') >= '2025-12-16'
    AND (start_time AT TIME ZONE 'Asia/Seoul') < '2025-12-20'
),
diffs AS (
  SELECT
    wl.id,
    wl.start_time,
    wl.end_time,
    ns.ts AS nearest_start_ts,
    ne.ts AS nearest_end_ts,
    abs(EXTRACT(EPOCH FROM (ns.ts - wl.start_time))) / 60.0 AS start_diff_min,
    abs(EXTRACT(EPOCH FROM (ne.ts - wl.end_time))) / 60.0 AS end_diff_min
  FROM wl
  LEFT JOIN LATERAL (
    SELECT u.ts
    FROM (
      (SELECT m."timestamp" AS ts
       FROM public.view_optimized_aligned_metrics m
       WHERE m."timestamp" <= wl.start_time
       ORDER BY m."timestamp" DESC
       LIMIT 1)
      UNION ALL
      (SELECT m."timestamp" AS ts
       FROM public.view_optimized_aligned_metrics m
       WHERE m."timestamp" >= wl.start_time
       ORDER BY m."timestamp" ASC
       LIMIT 1)
    ) u
    ORDER BY abs(EXTRACT(EPOCH FROM (u.ts - wl.start_time)))
    LIMIT 1
  ) ns ON true
  LEFT JOIN LATERAL (
    SELECT u.ts
    FROM (
      (SELECT m."timestamp" AS ts
       FROM public.view_optimized_aligned_metrics m
       WHERE wl.end_time IS NOT NULL
         AND m."timestamp" <= wl.end_time
       ORDER BY m."timestamp" DESC
       LIMIT 1)
      UNION ALL
      (SELECT m."timestamp" AS ts
       FROM public.view_optimized_aligned_metrics m
       WHERE wl.end_time IS NOT NULL
         AND m."timestamp" >= wl.end_time
       ORDER BY m."timestamp" ASC
       LIMIT 1)
    ) u
    ORDER BY abs(EXTRACT(EPOCH FROM (u.ts - wl.end_time)))
    LIMIT 1
  ) ne ON true
)
SELECT
  COUNT(*) AS row_count,
  COUNT(*) FILTER (WHERE nearest_start_ts IS NULL) AS start_no_match,
  COUNT(*) FILTER (WHERE nearest_end_ts IS NULL) AS end_no_match,
  COUNT(*) FILTER (WHERE start_diff_min > 20) AS start_over_20,
  COUNT(*) FILTER (WHERE end_diff_min > 20) AS end_over_20,
  COUNT(*) FILTER (WHERE start_diff_min > 30) AS start_over_30,
  COUNT(*) FILTER (WHERE end_diff_min > 30) AS end_over_30,
  MIN(start_diff_min) AS start_min,
  MAX(start_diff_min) AS start_max,
  percentile_cont(ARRAY[0.5,0.9,0.95,0.99]) WITHIN GROUP (ORDER BY start_diff_min) AS start_pcts,
  MIN(end_diff_min) AS end_min,
  MAX(end_diff_min) AS end_max,
  percentile_cont(ARRAY[0.5,0.9,0.95,0.99]) WITHIN GROUP (ORDER BY end_diff_min) AS end_pcts
FROM diffs;
