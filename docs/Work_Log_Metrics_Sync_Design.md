# tb_work_log 기반 die_id 동기화 스키마 설계

## 1. 요구사항 요약
- `view_optimized_aligned_metrics`의 **모든 컬럼을 복사**하되,
- **`die_id`만 `tb_work_log.die_id`로 치환**한 **새 스키마**를 만든다.
- 기존 `view_optimized_aligned_metrics`와 `tb_work_log`는 보존한다.

## 2. 입력 데이터 정의
- `all_metrics`: 장비 PLC에서 수집된 CSV 시계열 데이터.
- `view_optimized_aligned_metrics`: `all_metrics.temperature`를 left shift한 시계열 결과.
- `tb_work_log`: 오퍼레이터가 입력한 공정 단위 로그(행 1개 = 공정 1개).

## 3. 설계 목표
- 시계열 원본(`view_optimized_aligned_metrics`)은 유지하면서,
  **업무 기준 die_id**를 새 스키마에 반영한다.
- 조회 시스템(Grafana 등)이 **기존 뷰와 동일한 컬럼셋**을 사용하도록 유지한다.

## 4. 출력 스키마 선택지

### A안: Materialized View (권장, 단순/일관)
`mv_optimized_metrics_work_log`
- 컬럼: `view_optimized_aligned_metrics`와 동일.
- `die_id`만 work log 값으로 치환.
- 필요 시 `REFRESH MATERIALIZED VIEW`로 갱신.

### B안: 물리 테이블 스냅샷
`tb_optimized_metrics_work_log`
- MV 결과를 `CREATE TABLE AS SELECT`로 저장.
- 장점: 조회 성능 안정, 스냅샷 보관 용이.
- 단점: 증분 갱신 로직 별도 필요.

## 5. 매핑 로직 (핵심 규칙)

### 5.1 시간 범위 조인
```
metrics.timestamp >= work_log.start_time
AND metrics.timestamp <= work_log.end_time
```
- end_time은 **포함**으로 처리(요구사항 반영).

### 5.2 수기 입력 오차 보정(스냅)
- `tb_work_log`의 시간은 오차가 있을 수 있으므로 **허용 오차(±Δ)** 범위를 둔다.
- 허용 범위 안에서 **겹침이 가장 큰 시계열 구간**을 우선 매핑한다.
- 최소 겹침 기준(예: 5분 이상, 또는 겹침 비율 30% 이상)을 만족하지 못하면 **미매핑 처리**.

### 5.2 end_time이 NULL인 경우
- `end_time`이 NULL이면 다음 작업 시작 시각 또는 `now()`로 보정.
```
coalesce(end_time, next_start_time, now())
```

### 5.3 겹치는 work_log 우선순위
겹치는 작업 로그가 있을 때는 **단일 규칙**을 선택해야 한다.
권장 우선순위:
1) 최신 `created_at`
2) 동일 시 더 짧은 작업(구간 길이 짧음)
3) 동일 시 `id`가 큰 값

### 5.4 die_id 치환 정책
- 매핑 성공 시 **무조건 `tb_work_log.die_id`로 치환**.
- 매핑 실패 시:
  - 기본값: 기존 `metrics.die_id` 유지(데이터 손실 방지)
  - 대안: NULL 처리로 “미매핑” 표시

### 5.5 데이터가 없는 구간 처리
- 작업일보 구간에 시계열이 없으면 **새 스키마에 행이 생성되지 않음**.
- 대신 “미매핑 로그”로 기록하여 품질 모니터링에 활용.

### 5.6 부분 매핑(범위 초과/부족)
- 교집합 구간만 치환.
- 범위 밖은 기존 `metrics.die_id` 유지(또는 NULL 정책 적용).

## 6. MV 생성 SQL 스케치
```sql
CREATE MATERIALIZED VIEW public.mv_optimized_metrics_work_log AS
WITH work_log_ranges AS (
  SELECT
    id,
    die_id,
    start_time,
    COALESCE(
      end_time,
      LEAD(start_time) OVER (ORDER BY start_time),
      now()
    ) AS end_time,
    created_at
  FROM public.tb_work_log
)
SELECT
  COALESCE(w.die_id, m.die_id) AS die_id,
  m.timestamp,
  m.main_pressure,
  m.current_speed,
  m.billet_length,
  m.container_temp_front,
  m.container_temp_rear,
  m.production_counter,
  m.extrusion_end_position,
  m.mold_1,
  m.mold_2,
  m.mold_3,
  m.mold_4,
  m.mold_5,
  m.mold_6,
  m.billet_temp,
  m.at_pre,
  m.at_temp,
  m.billet_cycle_id,
  m.original_temperature,
  m.temperature,
  m._applied_offset,
  m.session_id,
  m.calc_cycle_id
FROM public.view_optimized_aligned_metrics m
LEFT JOIN LATERAL (
  SELECT wl.die_id
  FROM work_log_ranges wl
  WHERE m.timestamp >= wl.start_time
    AND m.timestamp <= wl.end_time
  ORDER BY wl.created_at DESC, wl.id DESC
  LIMIT 1
) w ON true
WITH NO DATA;
```

## 7. 운영/갱신 방식
- 순서: `view_optimized_aligned_metrics` REFRESH → `mv_optimized_metrics_work_log` REFRESH.
- 주기 갱신: 10분 또는 운영 상황에 맞춰 조정.
- 대량 구간 갱신 시 배치 처리 권장(시간 범위로 쪼개기).

## 8. 성능/인덱스 권장
- `view_optimized_aligned_metrics`에 `timestamp` 인덱스 유지.
- `tb_work_log`에 범위 조인 최적화를 위한 인덱스:
  - 단순: `btree(start_time, end_time)`
  - 고급: `tstzrange(start_time, end_time, '[)')` + `gist`

## 9. 검증 포인트
- 매핑 커버리지: 매핑 성공 비율(매핑된 metrics / 전체 metrics).
- 중복 매핑: 단일 timestamp에 2개 이상 매핑되는지 점검.
- die_id 치환 검증: 특정 공정 구간에서 die_id가 정확히 바뀌는지 샘플 확인.
 - 미매핑 로그: 작업일보가 있으나 매핑된 시계열이 없는 케이스 집계.

## 10. 추가 가정(중요)
- 현재 `view_optimized_aligned_metrics`에는 `machine_id`가 없음.
  - 다중 설비 환경이면 **machine_id 동기화 기준 추가**가 필요.
