SELECT COUNT(*) AS total_rows,
       COUNT(source_row) AS with_source_row,
       COUNT(*) FILTER (WHERE source_row IS NULL) AS null_source_row
FROM public.tb_work_log;

SELECT MIN(source_row) AS min_source_row,
       MAX(source_row) AS max_source_row
FROM public.tb_work_log;
