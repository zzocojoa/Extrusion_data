SELECT source_file, COUNT(*) AS row_count
FROM public.tb_work_log
GROUP BY 1
ORDER BY row_count DESC;

SELECT id,
       source_row,
       start_time,
       end_time,
       start_time AT TIME ZONE 'Asia/Seoul' AS start_kst,
       end_time AT TIME ZONE 'Asia/Seoul' AS end_kst,
       work_date,
       shift_type,
       data_quality_flags
FROM public.tb_work_log
WHERE source_file = '???? 2??(251223).xlsm'
  AND source_row BETWEEN 2487 AND 2494
ORDER BY source_row;
