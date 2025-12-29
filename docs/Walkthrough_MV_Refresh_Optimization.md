# Materialized View Refresh 성능 개선 가이드

## 1. 현재 병목 요약

`mv_optimized_metrics_work_log`는 `view_optimized_aligned_metrics`의 대량 시계열에 대해
`tb_work_log`를 LATERAL로 매칭한다. 이 구조는 **Row마다 범위 탐색 + 정렬 + LIMIT 1**이 반복되므로,
`REFRESH` 시 시간이 오래 걸린다.

## 2. 단기 개선(즉시 적용)

### 2-1) 인덱스/통계 확인

- `tb_work_log` 범위 매칭용 GIST 인덱스 확인
- 정렬 비용 줄이기 위한 Covering Index 적용
- 대량 변경 후 `ANALYZE` 실행

```sql
CREATE INDEX IF NOT EXISTS idx_work_log_time_range
    ON public.tb_work_log
    USING GIST (tstzrange(start_time, end_time, '[]'));

CREATE INDEX IF NOT EXISTS idx_work_log_cover_perf
    ON public.tb_work_log (start_time, end_time, created_at DESC)
    INCLUDE (die_id);

ANALYZE public.tb_work_log;
```

### 2-2) 작업 세션 튜닝

```sql
SET work_mem = '256MB'; -- 필요 시 상향
SET maintenance_work_mem = '512MB';
REFRESH MATERIALIZED VIEW public.mv_optimized_metrics_work_log;
```

### 2-3) CONCURRENTLY 사용 조건 점검

`REFRESH ... CONCURRENTLY`는 유니크 인덱스가 필요하다.
타임스탬프 중복이 있으면 실패하므로 사전 점검이 필요하다.

```sql
SELECT "timestamp", COUNT(*)
FROM public.view_optimized_aligned_metrics
GROUP BY 1
HAVING COUNT(*) > 1;
```

## 3. 중기 개선(쿼리 구조 개선)

### 3-1) Timeline Flattening

겹치는 `tb_work_log` 구간을 사전에 정리해 **비겹침 구간**으로 만들면,
최종 매칭은 단순 Range Join이 되어 `ORDER BY/LIMIT`가 사라진다.

```sql
-- 개념 예시
CREATE MATERIALIZED VIEW public.mv_work_log_ranges AS
SELECT DISTINCT ON (wl.start_time)
    id,
    die_id,
    start_time,
    end_time,
    tstzrange(start_time, end_time, '[]') AS period
FROM tb_work_log wl
ORDER BY wl.start_time, wl.created_at DESC;
```

### 3-2) 파티셔닝/BRIN 인덱스 고려

`view_optimized_aligned_metrics`가 매우 크면,
기반 테이블(all_metrics 등)에 날짜 파티셔닝 + BRIN 인덱스를 적용하면
범위 스캔 비용이 크게 줄어든다.

## 4. 장기 개선(증분 Refresh)

전체 `REFRESH` 대신 **증분 갱신 방식**으로 전환하는 것이 가장 효과적이다.

핵심 아이디어:
- 새로 들어온 시계열 구간만 INSERT
- 변경된 `tb_work_log` 구간만 DELETE + 재계산

```sql
-- 개념 예시: 변경된 기간만 재계산
WITH changed AS (
  SELECT MIN(start_time) AS from_ts, MAX(end_time) AS to_ts
  FROM public.tb_work_log
  WHERE updated_at >= now() - interval '1 hour'
)
DELETE FROM public.mv_optimized_metrics_work_log
WHERE "timestamp" BETWEEN (SELECT from_ts FROM changed)
                     AND (SELECT to_ts FROM changed);

INSERT INTO public.mv_optimized_metrics_work_log
SELECT ...
FROM public.view_optimized_aligned_metrics
WHERE "timestamp" BETWEEN (SELECT from_ts FROM changed)
                      AND (SELECT to_ts FROM changed);
```

## 5. 운영 체크리스트(실무 기준)

- `EXPLAIN (ANALYZE, BUFFERS)`로 인덱스 사용 여부 확인
- `pg_stat_statements`로 실행 시간 추적
- `REFRESH`는 비업무 시간대 예약
- `lock_timeout`/`statement_timeout`으로 안전장치 설정

```sql
SET lock_timeout = '5s';
SET statement_timeout = '0'; -- 대량 리프레시 시 제한 해제
```

## 요약

1. 단기: 인덱스 + work_mem 조정 + 통계 갱신
2. 중기: Timeline Flattening으로 LATERAL 제거
3. 장기: 증분 Refresh 전환
