SELECT source_file, COUNT(*) AS row_count
FROM public.tb_work_log
GROUP BY 1
ORDER BY row_count DESC;

SELECT source_row,
       start_time AT TIME ZONE 'Asia/Seoul' AS start_kst,
       end_time   AT TIME ZONE 'Asia/Seoul' AS end_kst,
       data_quality_flags
FROM public.tb_work_log
WHERE source_file = '압출일보 2호기(251224).xlsm'
  AND source_row BETWEEN 2487 AND 2494
ORDER BY source_row;
