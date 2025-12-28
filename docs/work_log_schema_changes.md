## Work Log 스키마 변경 내역 (2025-12-26)

### 변경 이유
- 원본 `압출일보 2호기(251223).xlsm`에서 `품명`은 사용하지 않기로 결정됨.
- `corrected_start_time`은 원본에 대응 컬럼이 없어 항상 null이며, 현재 로직에서 사용하지 않음.

### 적용 내용
- `tb_work_log`에서 `product_name` 컬럼 제거
- `tb_work_log`에서 `corrected_start_time` 컬럼 제거

### 적용 스크립트
```bash
psql "postgresql://postgres:postgres@127.0.0.1:25432/postgres" -f /mnt/c/Users/user/Documents/GitHub/Extrusion_data/supabase/migrations/20251226000002_drop_corrected_start_time.sql
```

```bash
psql "postgresql://postgres:postgres@127.0.0.1:25432/postgres" -f /mnt/c/Users/user/Documents/GitHub/Extrusion_data/supabase/migrations/20251226000003_drop_product_name.sql
```

### 관련 파일
- `supabase/migrations/20251226000001_restore_tb_work_log.sql`
- `supabase/migrations/20251226000002_drop_corrected_start_time.sql`
- `supabase/migrations/20251226000003_drop_product_name.sql`
