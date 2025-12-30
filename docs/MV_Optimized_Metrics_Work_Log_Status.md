# mv_optimized_metrics_work_log 진행 현황

## 목적
- `view_optimized_aligned_metrics`의 시계열 데이터에 대해 `tb_work_log`의 `die_id`를 동기화해 한 번에 조회.
- 매칭 실패 시 `view_optimized_aligned_metrics.die_id` 유지.

## 최신 스키마 정의(요약)
- 기준: `supabase/migrations/20251230000003_update_mv_optimized_metrics_work_log_use_ranges.sql`
- 조인 구조:
  1) **정확 매칭**: `mv_work_log_effective_ranges` 범위 조인
  2) **허용오차 매칭**: ±35분 tolerance LATERAL fallback
- 출력 `die_id`: `COALESCE(exact.die_id, tol.die_id, m.die_id)`

## 매칭 우선순위(허용오차 fallback)
- `match_priority`
  - 0: `m.timestamp`가 `wl.start_time ~ wl.end_time` 범위 안
  - 1: 범위 밖(허용오차로만 매칭)
- `gap_sec`: 범위 밖일 경우 시간 차이(초)
- 정렬 기준(선택 1건)
  1) `match_priority`
  2) `gap_sec`
  3) `wl.created_at DESC`
  4) `duration_sec ASC`
  5) `wl.id DESC`

## 성능 개선 적용 사항
- `tb_work_log` covering index 추가
  - `idx_work_log_cover_perf` (start_time, end_time, created_at DESC, id DESC) INCLUDE (die_id)
- 정확 매칭용 MV 추가
  - `mv_work_log_effective_ranges`
  - GIST 인덱스: `idx_mv_work_log_effective_ranges_period`
- `mv_optimized_metrics_work_log`는 **정확 매칭 우선**으로 경량화

## 증분 갱신(옵션)
전체 `REFRESH` 대신 **캐시 테이블 증분 갱신** 옵션 제공:

- 캐시 테이블
  - `mv_optimized_metrics_work_log_cache`
- 함수
  - `refresh_mv_work_log_effective_ranges()`
  - `refresh_mv_optimized_metrics_work_log_cache_full()`
  - `refresh_mv_optimized_metrics_work_log_cache_range(from_ts, to_ts, pad)`

> Grafana/분석에서는 필요 시 캐시 테이블을 사용하면 대량 REFRESH 부담이 감소한다.

## 리프레시/운영
- MV 생성 시 `WITH NO DATA` → 수동 `REFRESH` 필요
- `mv_work_log_effective_ranges` → `mv_optimized_metrics_work_log` 순서로 갱신 권장
- 캐시 테이블은 범위 단위 증분 갱신 가능

## 변경 이력(핵심)
- `20251230000001_add_work_log_cover_index.sql`
  - tb_work_log covering index 추가
- `20251230000002_create_mv_work_log_effective_ranges.sql`
  - 정확 매칭용 MV 추가
- `20251230000003_update_mv_optimized_metrics_work_log_use_ranges.sql`
  - MV 매칭 로직 개선
- `20251230000004_create_mv_optimized_metrics_work_log_cache.sql`
  - 증분 갱신 캐시/함수 추가

## 참고 파일
- `supabase/migrations/20251230000001_add_work_log_cover_index.sql`
- `supabase/migrations/20251230000002_create_mv_work_log_effective_ranges.sql`
- `supabase/migrations/20251230000003_update_mv_optimized_metrics_work_log_use_ranges.sql`
- `supabase/migrations/20251230000004_create_mv_optimized_metrics_work_log_cache.sql`
