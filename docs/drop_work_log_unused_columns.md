## tb_work_log 불필요 컬럼 삭제

아래 명령을 한 줄씩 실행하세요.

```bash
psql "postgresql://postgres:postgres@127.0.0.1:25432/postgres" -f /mnt/c/Users/user/Documents/GitHub/Extrusion_data/supabase/migrations/20251226000002_drop_corrected_start_time.sql
```

```bash
psql "postgresql://postgres:postgres@127.0.0.1:25432/postgres" -f /mnt/c/Users/user/Documents/GitHub/Extrusion_data/supabase/migrations/20251226000003_drop_product_name.sql
```

확인:

```bash
psql "postgresql://postgres:postgres@127.0.0.1:25432/postgres" -c "select column_name from information_schema.columns where table_schema='public' and table_name='tb_work_log' and column_name in ('product_name','corrected_start_time');"
```
