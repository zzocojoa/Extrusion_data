# MV Refresh Runbook

## 기본 리프레시 순서

1. 정확 매칭 범위 갱신

```sql
REFRESH MATERIALIZED VIEW public.mv_work_log_effective_ranges;
```

2. 최종 MV 갱신

```sql
REFRESH MATERIALIZED VIEW public.mv_optimized_metrics_work_log;
```

## 한 번에 실행(권장)

```sql
REFRESH MATERIALIZED VIEW public.mv_work_log_effective_ranges;
REFRESH MATERIALIZED VIEW public.mv_optimized_metrics_work_log;
```

## 자동 리프레시(10분 간격, 잡 분리)

```sql
CREATE EXTENSION IF NOT EXISTS pg_cron;

SELECT cron.schedule(
  'refresh_mv_work_log_ranges_10m',
  '*/10 * * * *',
  'REFRESH MATERIALIZED VIEW public.mv_work_log_effective_ranges'
);

SELECT cron.schedule(
  'refresh_mv_work_log_10m_concurrent',
  '2-59/10 * * * *',
  'REFRESH MATERIALIZED VIEW CONCURRENTLY public.mv_optimized_metrics_work_log'
);
```

## cron.job 조회

```sql
SELECT jobid, jobname, schedule, command, nodename, nodeport, database, username, active
FROM cron.job
ORDER BY jobid;
```

## 증분/캐시 갱신(옵션)

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

## 참고

- `mv_work_log_effective_ranges` → `mv_optimized_metrics_work_log` 순서가
  중요하다.
- `CONCURRENTLY`는 유니크 인덱스 조건이 맞을 때만 사용한다.
