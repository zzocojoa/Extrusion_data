SELECT
  id,
  die_id,
  die_number,
  start_time,
  end_time,
  start_time AT TIME ZONE 'Asia/Seoul' AS start_kst,
  end_time AT TIME ZONE 'Asia/Seoul' AS end_kst
FROM public.tb_work_log
WHERE die_id = '60271'
  AND (start_time AT TIME ZONE 'Asia/Seoul') >= '2025-12-17 00:00'
  AND (start_time AT TIME ZONE 'Asia/Seoul') < '2025-12-20 00:00'
ORDER BY start_time;
