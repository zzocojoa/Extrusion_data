SELECT source_file, COUNT(*) AS row_count
FROM public.tb_work_log
GROUP BY 1
ORDER BY row_count DESC;

SELECT MIN(start_time AT TIME ZONE 'Asia/Seoul') AS min_start_kst,
       MAX(end_time   AT TIME ZONE 'Asia/Seoul') AS max_end_kst
FROM public.tb_work_log;

SELECT date_part('year', start_time AT TIME ZONE 'Asia/Seoul') AS year,
       COUNT(*) AS row_count
FROM public.tb_work_log
GROUP BY 1
ORDER BY 1;
