# mv_optimized_metrics_work_log 진행 현황

## 목적
- `view_optimized_aligned_metrics`의 시계열 데이터에 대해 `tb_work_log`의 `die_id`를 동기화해 한 번에 조회할 수 있도록 만든 머티리얼라이즈드 뷰.
- 매칭 실패 시에는 기존 `view_optimized_aligned_metrics.die_id`를 유지.

## 최신 스키마 정의(요약)
- 기준: `supabase/migrations/20251228000005_update_mv_work_log_tolerance.sql`
- 조인 방식: `view_optimized_aligned_metrics m` + `tb_work_log wl`의 **LATERAL 조인**
- 매칭 범위: `wl.start_time - 35분` ~ `wl.end_time + 35분`
- 출력 `die_id`: `COALESCE(w.die_id, m.die_id)`

## 매칭 우선순위(현재 로직)
- `match_priority`
  - 0: `m.timestamp`가 `wl.start_time ~ wl.end_time` 범위 안
  - 1: 범위 밖(허용오차로만 매칭)
- `gap_sec`: 범위 밖일 경우 시간 차이(초)
- 정렬 기준(선택 1건)
  1) `match_priority` (정확 매칭 우선)
  2) `gap_sec` (가장 가까운 구간)
  3) `wl.created_at DESC`
  4) `duration_sec ASC`
  5) `wl.id DESC`

## 인덱스/성능
- `tb_work_log`에 GIST 범위 인덱스 생성:
  - `idx_work_log_time_range` (tstzrange start_time~end_time)
- `mv_optimized_metrics_work_log`에 유니크 인덱스:
  - `idx_mv_optimized_metrics_work_log_ts` (timestamp)
  - `REFRESH MATERIALIZED VIEW CONCURRENTLY`를 위해 필요

## 리프레시/운영
- 뷰 생성 시 `WITH NO DATA`로 생성 → 수동 `REFRESH` 필요
- 현재 상태 확인 결과:
  - `pg_matviews.ispopulated = true`
  - 유니크 인덱스 존재 확인

## 변경 이력(핵심)
- `20251228000003_create_mv_optimized_metrics_work_log.sql`
  - 기본 LATERAL 매칭(정확 범위)
- `20251228000004_optimize_mv_work_log.sql`
  - GIST range 인덱스 도입
- `20251228000005_update_mv_work_log_tolerance.sql`
  - ±35분 허용오차 매칭 및 우선순위 로직 추가
- `20251228000006_add_unique_index_mv_work_log.sql`
  - `timestamp` 유니크 인덱스(동시 리프레시 지원)

## 현재 상태 요약
- 스키마/뷰/인덱스 모두 반영됨
- 허용오차(±35분) 및 우선순위 로직 적용됨
- 동시 리프레시가 가능한 상태

## 참고 파일
- `supabase/migrations/20251228000003_create_mv_optimized_metrics_work_log.sql`
- `supabase/migrations/20251228000004_optimize_mv_work_log.sql`
- `supabase/migrations/20251228000005_update_mv_work_log_tolerance.sql`
- `supabase/migrations/20251228000006_add_unique_index_mv_work_log.sql`
