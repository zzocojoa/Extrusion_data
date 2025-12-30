# mv_optimized_metrics_work_log REFRESH 성능 개선 가이드

## 목표
- `REFRESH MATERIALIZED VIEW` 시간 단축
- `tb_work_log` 보정/오차가 있는 상태에서도 매칭 정확도 유지
- 전체 리프레시가 어려울 경우 **증분 갱신** 옵션 제공

## 1) 구조 개선 개요

### 기존 병목
- `view_optimized_aligned_metrics`의 대량 시계열에 대해
  `tb_work_log`를 **Row마다 LATERAL + ORDER BY + LIMIT 1**로 매칭
- 전체 리프레시가 오래 걸림

### 개선 방향
1. **정확 매칭 범위**를 별도 MV로 미리 계산
2. 정확 매칭은 범위 조인으로 처리
3. 허용오차(±35분)는 **필요할 때만** fallback
4. 전체 리프레시가 부담이면 **캐시 테이블 증분 갱신** 사용

## 2) 적용된 스키마 변경

### A. tb_work_log covering index

```sql
CREATE INDEX IF NOT EXISTS idx_work_log_cover_perf
    ON public.tb_work_log (start_time, end_time, created_at DESC, id DESC)
    INCLUDE (die_id);
```

### B. 정확 매칭용 MV

- 파일: `supabase/migrations/20251230000002_create_mv_work_log_effective_ranges.sql`
- 목적: 겹치는 구간을 정리해 **비겹침 범위**로 변환

```sql
REFRESH MATERIALIZED VIEW public.mv_work_log_effective_ranges;
```

### C. mv_optimized_metrics_work_log 개선

- 파일: `supabase/migrations/20251230000003_update_mv_optimized_metrics_work_log_use_ranges.sql`
- 매칭 순서
  1) `mv_work_log_effective_ranges`로 정확 매칭
  2) 실패 시 ±35분 tolerance fallback

```sql
REFRESH MATERIALIZED VIEW public.mv_optimized_metrics_work_log;
```

## 3) 증분 갱신(옵션)

전체 리프레시가 부담이면 **캐시 테이블**로 증분 갱신을 한다.

- 캐시 테이블: `public.mv_optimized_metrics_work_log_cache`
- 함수:
  - `refresh_mv_work_log_effective_ranges()`
  - `refresh_mv_optimized_metrics_work_log_cache_full()`
  - `refresh_mv_optimized_metrics_work_log_cache_range(from_ts, to_ts, pad)`

### 전체 갱신

```sql
SELECT public.refresh_mv_optimized_metrics_work_log_cache_full();
```

### 범위 갱신

```sql
SELECT public.refresh_mv_optimized_metrics_work_log_cache_range(
  '2025-12-19 00:00:00+09',
  '2025-12-19 23:59:59+09',
  interval '35 min'
);
```

## 4) 운영 체크리스트

- `mv_work_log_effective_ranges` → `mv_optimized_metrics_work_log` 순서로 갱신
- 대량 리프레시 전 `work_mem` 상향
- 증분 갱신을 운영하면 Grafana는 캐시 테이블을 조회

```sql
SET work_mem = '256MB';
```

## 요약

1. 정확 매칭을 MV로 분리해 리프레시 비용 감소
2. 허용오차는 fallback으로 유지
3. 필요 시 캐시 테이블로 증분 갱신 지원
