SELECT COUNT(*) AS total_rows,
       COUNT(source_file) AS with_source_file,
       COUNT(*) FILTER (WHERE source_file IS NULL) AS null_source_file
FROM public.tb_work_log;

SELECT source_file, COUNT(*) AS row_count
FROM public.tb_work_log
GROUP BY 1
ORDER BY row_count DESC
LIMIT 20;

SELECT date_part('year', start_time AT TIME ZONE 'Asia/Seoul') AS year,
       COUNT(*) AS row_count
FROM public.tb_work_log
GROUP BY 1
ORDER BY 1;

SELECT id,
       start_time,
       end_time,
       start_time AT TIME ZONE 'Asia/Seoul' AS start_kst,
       end_time AT TIME ZONE 'Asia/Seoul' AS end_kst,
       source_file,
       source_row,
       data_quality_flags
FROM public.tb_work_log
WHERE start_time >= '2026-01-01' OR end_time >= '2026-01-01'
ORDER BY start_time
LIMIT 50;
