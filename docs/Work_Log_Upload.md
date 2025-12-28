# 작업일보 업로드 탭 로직 정리

현재 프로그램에서 “Work Log(작업일보) 업로드” 탭에 연결된 UI/파싱/업로드 흐름을 요약한다.

## 1) UI 흐름 (GUI)
- 사이드바 `Work Log` 버튼 클릭 시 작업일보 업로드 화면을 구성한다: `uploader_gui_tk.py`
- 파일 선택 버튼으로 Excel 파일(`.xlsx/.xls/.xlsm`)을 고른다.
  - 선택 전까지 업로드 버튼은 비활성화.
- 업로드 시작 시 백그라운드 스레드에서 파싱 → 업로드를 실행한다.
  - 성공/실패 메시지는 텍스트 박스와 팝업(`messagebox`)으로 안내.
- 로그는 `log_to_box()`를 통해 탭 내 텍스트 박스에 누적된다.

관련 코드:
- `uploader_gui_tk.py` (`show_work_log`, `on_select_work_log`, `on_upload_work_log`)

## 2) Excel 파싱 로직
파싱은 `core/work_log.py`의 `parse_work_log_excel()`에서 수행한다.

### 2.1 시트/헤더 탐색
- 모든 시트의 상단 20행을 훑어 “공장” 문자열이 있는 행을 헤더로 판단한다.
- 해당 시트를 헤더 행 기준으로 다시 읽고, 컬럼명 공백/개행을 정리한다.

### 2.2 필수 컬럼
- 필수: `날짜`, `시작`, `종료`, `공장`
- 없으면 즉시 오류를 발생시킨다.

### 2.3 시간 파싱
- `날짜` + `시작/종료`를 조합해 KST 기준의 `start_time`, `end_time`을 생성한다.
- 종료 시간이 시작 시간보다 이르면 다음 날로 이월.
- 시작·종료가 동일하면 최소 1분을 더한다.

### 2.4 데이터 정규화/반올림
- 숫자 컬럼은 콤마 제거 → 숫자 변환.
- 반올림 규칙:
  - `수율`: 소수 1자리 유지
  - `생산성`: 1자리 반올림 후 정수화
  - `적합중량`: 1자리 반올림 후 정수화
- `#`는 다이 번호(정수)로 정규화한다.

### 2.5 컬럼 선택 및 매핑
- 선택 컬럼(있는 경우만):
  - `machine_id`, `start_time`, `end_time`, `생산자`, `DW No.`, `재질`, `LOT`, `질별`,
    `온도`, `퀜칭온도`, `출구온도`, `스트레칭`, `중량`, `RAM`, `길이`, `실단중`,
    `적합수량`, `적합중량`, `생산성`, `#`, `수율`, `기포`, `뜯김`, `백선/흑선`,
    `산화물`, `스크래치`, `휨`, `치수`, `라인`, `기타`, `S`, `E`,
    `OP Note (특이사항 입력란)`
- DB 컬럼 매핑:
  - `생산자` → `worker_name`
  - `DW No.` → `die_id`
  - `재질` → `alloy_type`
  - `LOT` → `lot`
  - `질별` → `temper_type`
  - `온도` → `target_billet_temp`
  - `퀜칭온도` → `quenching_temp`
  - `출구온도` → `target_exit_temp`
  - `스트레칭` → `stretching`
  - `중량` → `total_weight`
  - `RAM` → `ram`
  - `길이` → `product_length`
  - `실단중` → `actual_unit_weight`
  - `적합수량` → `production_qty`
  - `적합중량` → `production_weight`
  - `생산성` → `productivity`
  - `#` → `die_number`
  - `수율` → `yield_rate`
  - `기포` → `defect_bubble`
  - `뜯김` → `defect_tearing`
  - `백선/흑선` → `defect_white_black_line`
  - `산화물` → `defect_oxide`
  - `스크래치` → `defect_scratch`
  - `휨` → `defect_bend`
  - `치수` → `defect_dimension`
  - `라인` → `defect_line`
  - `기타` → `defect_etc`
  - `S` → `start_cut`
  - `E` → `end_cut`
  - `OP Note (특이사항 입력란)` → `op_note`
- `production_qty`, `die_number`, `product_length`, `start_cut`, `end_cut`, 결함 수량 컬럼은 `Int64`로 캐스팅한다.

관련 코드:
- `core/work_log.py`

> 참고: `core/work_log.py`에는 동일 함수 정의가 중복되어 있으며, 파싱 로직은 마지막 정의 기준으로 동작한다.

## 3) 업로드 로직 (중복 방지 포함)
업로드는 `core/upload.py`의 `upload_work_log_data()`에서 수행한다.

### 3.1 REST 업로드
- 대상 테이블: `tb_work_log`
- 호출 URL: `${SUPABASE_URL}/rest/v1/tb_work_log`
- 헤더: `apikey` + `Authorization: Bearer <anon_key>`
- 타임스탬프는 ISO 문자열로 변환한 뒤 전송한다.

### 3.2 중복 방지(스마트 필터)
- 기존 데이터 조회 후 중복 제거(업로드 전):
  - `start_time`, `machine_id`, `die_number`, `production_qty`,
    `production_weight`, `productivity` + 추가 컬럼
    (`lot`, `temper_type`, `quenching_temp`, `stretching`, `total_weight`, `ram`,
    `product_length`, `actual_unit_weight`, 결함 수량, `start_cut`, `end_cut`,
    `op_note`) 조합을 시그니처로 사용.
  - DB와 업로드 데이터를 UTC로 정규화해 비교.
- 중복 체크 실패 시 **Fail-Close**로 업로드 중단.

### 3.3 결과 처리
- 업로드 성공 시 “성공/건수” 로그 출력.
- 실패 시 오류 메시지 출력 후 종료.

관련 코드:
- `core/upload.py`

## 4) 업로드 데이터의 후속 사용처
업로드된 작업일보는 사이클 분석 시 매칭되어 `tb_cycle_log`에 연결된다.
- `core/cycle_processing.py`에서 작업 구간에 해당하는 `work_log_id`를 매핑한다.

## 5) 관련 파일
- `uploader_gui_tk.py`
- `core/work_log.py`
- `core/upload.py`
- `core/cycle_processing.py`

## 6) 실데이터 컬럼 확인 (압출일보 2호기(251223).xlsm)
대상 파일: `data/raw/압출일보 2호기(251223).xlsm`

- 시트: 첫 번째 시트(인덱스 0, 보통 “창녕 압출일보”)
- 헤더 행: 0‑based 기준 3행(= 엑셀 4행)
- 원본 헤더에 `출구온도`는 없고, 숫자 컬럼 `525`가 존재한다.
  - `core/work_log.py`는 `525` → `출구온도`로 rename 하므로 이 파일은 정상 매핑된다.
  - 만약 컬럼명이 `525`가 아니면 `출구온도` 매핑이 누락될 수 있다.
- 원본 헤더에 `스트레칭`은 없고, 숫자 컬럼 `80`이 존재한다.
  - `core/work_log.py`는 `80` → `스트레칭`으로 rename 한다.
- `길이` 컬럼이 두 번 존재한다. pandas 로딩 시 두 번째는 `길이.1`로 표기된다.
  - 실제 제품 길이는 `RAM`과 `이론단중` 사이의 첫 번째 `길이`를 사용한다.

### 6.1 미반영(현 로직에서 사용하지 않는) 컬럼
아래 컬럼은 원본에 존재하지만 현재 “컬럼 선택 및 매핑”에 포함되지 않는다.

- `주/야`
- `양산구분`
- `업체명`
- `규격`, `수량`
- `규격2`, `수량2`
- `규격3`, `수량3`
- `규격4`, `수량4`
- `규격5`, `수량5`
- `빈칸`
- `업체`
- `품명`
- `이론단중`
- `부적합수량`
- `부적합중량`
- `소요`
- `길이.1` (중복 컬럼)
- `Unnamed: 55` ~ `Unnamed: 60` (빈 컬럼)

## 7) 업로드 파일 형식 지원 여부
- **작업일보 업로드 탭**: Excel만 지원 (`.xlsx`, `.xls`, `.xlsm`)
  - GUI에서 파일 선택 시 Excel 확장자만 허용됨.
- **일반 데이터(PLC) 업로드**: CSV만 지원
  - 파일 후보 수집 로직이 `.csv`만 대상으로 함.
