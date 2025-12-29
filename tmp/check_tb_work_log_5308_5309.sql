SELECT
  id,
  start_time,
  end_time,
  start_time AT TIME ZONE 'Asia/Seoul' AS start_kst,
  end_time AT TIME ZONE 'Asia/Seoul' AS end_kst,
  work_date,
  shift_type,
  source_file,
  source_row
FROM public.tb_work_log
WHERE id IN (5308, 5309)
ORDER BY id;
