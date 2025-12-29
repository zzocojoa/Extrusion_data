SELECT source_row,
       start_time AT TIME ZONE 'Asia/Seoul' AS start_kst,
       end_time   AT TIME ZONE 'Asia/Seoul' AS end_kst,
       data_quality_flags
FROM public.tb_work_log
WHERE source_file = '압출일보 2호기(251224).xlsm'
  AND source_row = 2469;
