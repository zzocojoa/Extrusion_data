# Cycle Operations

## 목적

Extrusion Uploader의 cycle 기능은 정식 canonical 경로와 운영자 전용 legacy backfill 경로를 분리해 운영한다.

- 정식 경로: `view_optimized_aligned_metrics -> mv_work_log_effective_ranges -> mv_optimized_metrics_work_log -> mv_optimized_metrics_work_log_cache`
- snapshot 저장: `tb_cycle_log`
- legacy 경로: `core/cycle_processing.py`

`tb_cycle_log`는 source of truth가 아니라 snapshot/compatibility sink다. 정식 계산 기준은 canonical cache다.

## GUI 위치

- 일반 사용자:
  - `Cycle Ops` 화면에서 `Canonical Refresh`, `Snapshot Sync`, `Parity / Health`만 사용한다.
- 운영자:
  - 필요할 때만 `Legacy Cycle Backfill`을 사용한다.
- `Data Mgmt`:
  - training dataset builder와 archive만 제공한다.

## 정식 흐름

### 1. Canonical Refresh

다음 객체를 순서대로 갱신한다.

1. `public.view_optimized_aligned_metrics`
2. `public.mv_work_log_effective_ranges`
3. `public.mv_optimized_metrics_work_log`
4. `public.refresh_mv_optimized_metrics_work_log_cache_full()`

이 단계의 목적은 cycle 계산 기준 데이터를 최신 상태로 맞추는 것이다.

### 2. Snapshot Sync

`mv_optimized_metrics_work_log_cache`에서 `(die_id, session_id, calc_cycle_id)` 단위로 cycle 경계를 집계한다.

저장 컬럼은 다음과 같다.

- `machine_id`
- `start_time`
- `end_time`
- `production_counter`
- `work_log_id`
- `duration_sec`
- `max_pressure`
- `is_valid`
- `is_test_run`
- `source_mode`
- `algorithm_version`

canonical sync는 아래 값으로 적재한다.

- `source_mode='canonical_snapshot'`
- `algorithm_version='aligned_metrics_v1'`

canonical sync는 매 실행마다 기존 canonical snapshot을 먼저 비우고 다시 적재한다. legacy backfill row는 유지한다.

`work_log_id`가 매핑되지 않으면 `machine_id='[unmapped]'`로 저장한다.

### 3. Parity / Health

운영 화면에서 다음 지표를 확인한다.

- canonical cache row 수
- canonical cache cycle 수
- cache timestamp 범위
- snapshot row 수
- snapshot canonical row 수
- snapshot legacy row 수
- snapshot unmapped row 수
- snapshot 최신 cycle 종료 시각
- snapshot 최신 업데이트 시각

## Legacy Backfill

`core/cycle_processing.py`는 pressure threshold 기반의 기존 Python 분할 로직을 유지한다. 이 경로는 정식 사용자 기능이 아니라 운영자 전용 backfill 도구다.

- 입력:
  - `machine_id`
  - `incremental | all | today | yesterday | custom`
  - `custom`일 때 시작일
- 저장:
  - `tb_cycle_log`
- 저장 표시:
  - `source_mode='legacy_backfill'`
  - `algorithm_version='pressure_threshold_v1'`

주의:

- first-run incremental은 기존 legacy snapshot이 없으면 중단하고 full backfill이 먼저 필요하다고 표시한다.
- 중복 방지는 `tb_cycle_log(machine_id, start_time, end_time, source_mode, algorithm_version)` unique key + upsert에 의존한다.
- legacy range 처리에는 chunk 경계 리스크가 있으므로 정식 운영 지표로 사용하지 않는다.

## DB 설정

cycle / archive 운영 기능은 다음 설정 키를 사용한다.

- `DB_HOST`
- `DB_PORT`
- `DB_USER`
- `DB_PASSWORD`
- `DB_NAME`

기본 host/user/dbname은 코드 기본값을 사용한다. `DB_PORT`가 비어 있으면 `supabase/config.toml`의 로컬 포트를 읽는다. 값 오버라이드는 `.env -> os.environ` 순서를 따른다. `LEGACY_CYCLE_MACHINE_ID`를 설정하면 `Cycle Ops` 화면의 기본 `machine_id`로 사용한다.

## Training과의 관계

`training_base`와 `training_dataset_v1`는 `tb_cycle_log`를 직접 읽지 않는다. 다만 raw CSV lineage 위에 integrated log 기준 `die_id`, `billet_cycle_id`, cycle 관련 label 파생값이 포함될 수 있다.

후속 작업이 필요하다면 canonical cache 또는 정식 MV를 기반으로 별도 training export 경로를 설계한다.
