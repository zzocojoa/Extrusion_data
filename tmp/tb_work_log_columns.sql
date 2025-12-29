SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'public' AND table_name = 'tb_work_log'
ORDER BY ordinal_position;
