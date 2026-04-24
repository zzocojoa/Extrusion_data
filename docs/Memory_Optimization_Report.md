# 메모리 최적화 리포트 (Extrusion_data)

## 1. 범위
- 대상: GUI 업로더, Work Log Excel 파서, CSV 변환 파이프라인, 사이클 처리
- 목적: 대용량 파일/장시간 업로드 시 메모리 피크를 낮추고 안정성 확보

## 2. 메모리 사용이 큰 지점 (현상/원인)
- `core/work_log.py` `parse_work_log_excel()`
  - 모든 시트를 훑고, 실제 시트를 전체 로드하여 `DataFrame` 생성.
  - `df.apply(axis=1)` 기반 timestamp 조합은 행 단위 객체 생성으로 메모리/CPU 동반 상승.
  - `read_excel()`은 내부적으로 워크북 전체를 메모리에 올리는 특성이 있음.
- `core/upload.py` `upload_work_log_data()`
  - `df.copy()` + `astype(object)` + `to_dict()`로 한 번에 전체 JSON 생성.
  - 중복 방지용 `existing_signatures` 집합이 전체 기존 데이터(특정 machine_id 전부)를 메모리로 로딩.
- `uploader_gui_tk.py` `preview_diagnostics()`
  - 미리보기 단계에서 파일마다 전체 CSV 파싱 → 여러 파일일 때 피크 증가.
- `core/transform.py` `build_records_plc()/build_records_temp()`
  - CSV 전체 컬럼을 읽고 dtype 추론(객체 컬럼 많을수록 메모리 증가).
  - chunking은 적용됐지만 `usecols` 미적용으로 불필요 컬럼까지 메모리에 적재.
- `core/cycle_processing.py`
  - 범위가 길면 `read_sql()` 결과가 한 번에 메모리 적재.
  - 컬럼 추가/슬라이싱으로 내부 복사 발생.
- `uploader_gui_tk.py`
  - `ThreadPoolExecutor(max_workers=4)`로 여러 파일 동시 파싱 시 메모리 동시 사용량 증가.

## 3. 우선순위 개선안 (실무 관점)

### A. 즉시 효과 큰 개선 (High Impact)
1) **Work Log 업로드: 서버 중복 방지로 전환**
   - DB에 유니크 인덱스/키를 두고 `on_conflict` 또는 `Prefer: resolution=ignore-duplicates` 사용.
   - 클라이언트에서 `existing_signatures` 전량 로딩 제거 → 메모리 급감.
2) **Work Log 업로드: 배치 전송**
   - `to_dict()` 한 번에 만들지 말고 300~500행 배치로 JSON 생성/전송.
3) **Work Log Excel: `usecols` 적용**
   - 실제 매핑된 컬럼만 읽도록 `read_excel(usecols=...)` 사용.
   - `openpyxl` `read_only=True`로 헤더 위치 탐색(시트 전체 로드 방지).

### B. 중간 난이도/효과 (Medium)
1) **CSV 처리 최적화**
   - `build_records_plc()`에서 컬럼 후보를 먼저 추출 후 `usecols`로 재읽기.
   - 숫자 컬럼 dtype 지정(`float64`/`Int64`)으로 객체 컬럼 최소화.
2) **사이클 처리 chunking**
   - `pd.read_sql_query(..., chunksize=N)`로 스트리밍 처리.
   - `process_chunk()`에서 불필요 컬럼 생성 최소화(NumPy array 기반 계산).
3) **GUI 동시 처리 제한**
   - 파일 크기 기준으로 동시 작업 수 조절(예: 200MB 이상이면 단일 처리).

### C. 구조적 개선 (Long Term)
1) **Excel/CSV 스트리밍 파서**
   - Excel은 `openpyxl` row iterator로 스트리밍 파싱.
   - CSV는 `pyarrow` 또는 `polars` 기반 스트리밍 도입(옵션 라이브러리).
2) **DB 직접 적재**
   - REST JSON 대신 `COPY`/`psycopg` bulk insert로 전송 데이터 직렬화 비용 절감.

## 4. 개선 실행안 (현 코드 기준 제안)

### 4.1 Work Log 업로드 배치화
- `upload_work_log_data()`를 `batch_size` 단위로 split 후 업로드.
- 각 배치에서만 `to_dict()` 수행 → 피크 메모리 감소.

### 4.2 Work Log 중복 제거 전략 변경
- 서버 단에서 유니크 키로 중복 방지:
  - 예: `(machine_id, start_time, die_number, production_qty, production_weight, productivity, lot, temper_type, ...)`
- 클라이언트는 중복 체크 대신 "재전송 허용" 구조로 단순화.

### 4.3 Excel 읽기 최소화
- 헤더 탐색: `openpyxl` `read_only=True`로 1~20행만 확인.
- 실제 데이터 로딩: `read_excel(usecols=[필수 컬럼])`.

## 5. 측정/검증 방법
- **메모리 피크 측정**: `psutil.Process().memory_info().rss` 또는 `tracemalloc`
- **로드/업로드 전후 비교 로그**:
  - Excel 로딩 직후
  - JSON 변환 직후
  - 업로드 배치 완료 시점
- **대용량 시나리오**(수십만 행 CSV, 수만 행 Excel) 기준으로 피크 확인

## 6. 결론
- 가장 큰 메모리 원인은 **Work Log 중복 체크 + 전체 JSON 생성**이며,
  이를 **서버 중복 방지 + 배치 전송**으로 전환하면 피크 메모리 감소 폭이 큽니다.
- Excel/CSV 파싱은 `usecols` + chunking 적용만으로도 안정성 향상이 가능합니다.
