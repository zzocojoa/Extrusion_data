# 프로젝트 컨텍스트 및 환경 요약 (Master v4.1 - 최종 최적화)

Extrusion Uploader는 GUI/EXE 중심으로 운영하며, 사용자 진입점은
`ExtrusionUploader.exe` 또는 `uploader_gui_tk.py`다. CLI 기반 업로더는 제거되었고,
남은 스크립트는 로컬 Supabase, 백업, 복구, 데이터 보관을 돕는 내부/운영 보조용이다.

**중요:** 이 프로젝트는 엄격한 네트워크/인증 규칙이 있는 특정 로컬 Docker
구성을 사용한다. 이 설정과 충돌하는 일반적인 해법을 제안하지 말 것.

## 1. 인프라 (로컬 하이브리드)

- **호스트 OS:** Windows 10/11 (WSL2 백엔드 활성화)

- **게스트 OS(서버):** Ubuntu (WSL2에서 실행)

- **물리 스토리지:**
  - **데이터베이스:** WSL 루트 파일시스템 (`ext4.vhdx`).
  - **백업 스토리지:** `.env`의 `BACKUP_DIR`로 지정한 외부 경로.
    - **권장:** NAS 또는 별도 외장 저장소.
    - **주의:** 실시간 DB 저장소와 백업 저장소는 분리한다.

## 2. 기술 스택 & 네트워크 (고정)

- **도구:** Supabase CLI는 로컬 Supabase, 백업, 복구 운영을 위한 내부 보조용으로만
  사용한다. 업로더 사용자 흐름에는 포함하지 않으며, GUI의 Start Upload와 Data Mgmt가
  주요 사용자 진입점이다.

- **공유 네트워크:** `supabase_network_Extrusion_data`

- **데이터베이스 (Supabase):**

  - 컨테이너 이름: `supabase_db_Extrusion_data`

  - **외부 포트(호스트/DBeaver):** `25432` (`config.toml`에서 매핑)

  - **내부 포트(Docker/Grafana):** `5432`

- **시각화 (Grafana):**

  - 컨테이너 이름: `grafana_local`

  - 연결 호스트: `supabase_db_Extrusion_data:5432` (컨테이너 간 직접 연결)

  - URL: `http://localhost:3001`

## 3. 클라이언트 설정 (Python GUI)

- **파일:** `uploader_gui_tk.py`, `core/config.py`
- **사용자 경로:** GUI와 EXE만 사용한다. CLI 사용법은 문서와 운영 안내에서 제외한다.
- **아카이브 경로:** Data Mgmt 화면에서 미리보기, export, 검증 후 삭제를 GUI로 처리한다.
- **Cycle 운영 경로:** Cycle 기능은 `Data Mgmt`가 아니라 별도 `Cycle Ops` 화면에서 실행한다.
- **Cycle 정식 경로:** 정식 계산은 `view_optimized_aligned_metrics -> mv_work_log_effective_ranges -> mv_optimized_metrics_work_log_cache` 기준으로 본다.
- **Cycle legacy 경로:** `core/cycle_processing.py`는 운영자 전용 backfill 도구이며 정식 사용자 흐름에서 제외한다.

- **설정 관리:** 기본은 AppData의 `%APPDATA%\\ExtrusionUploader\\config.ini`이며, 개발 실행에서는 저장소 루트 `.env`, 배포본에서는 실행 파일 옆 `.env`, 그리고 `os.environ`으로 override할 수 있다.
- **UI 언어:** `UI_LANGUAGE`로 관리하며, 기본값은 `ko`다. `ko`와 `en`은 i18n 리소스로 분리해 관리하고, 배포본에서는 이 리소스가 exe와 함께 포함되는 구성을 전제로 한다.
- **초기화:** AppData 설정 파일이 없으면 루트 `config.ini`를 복사해 사용한다.
- **적용 우선순위:** `기본값 -> AppData config.ini -> .env -> os.environ`
- **언어 우선순위:** `UI_LANGUAGE`는 AppData `config.ini`를 우선 적용하고, 없으면 기본값 `ko`를 사용한다.
- **Settings 주의:** `Edge URL`의 `비우기`는 입력값만 지우며, `설정 저장` 후 자동 계산값이 적용된다.
- **옵션 지속성:** `SMART_SYNC`, `AUTO_UPLOAD`도 AppData/.env/os.environ 우선순위로 함께 로드된다.
- **custom 범위:** `RANGE_MODE=custom`이면 `CUSTOM_DATE_START`, `CUSTOM_DATE_END`를 함께 사용하며, GUI에서는 커스텀 달력으로 입력할 수 있다.
- **DB 연결 설정:** cycle / archive 운영 기능은 `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, `DB_NAME`을 사용한다. 기본 host/user/dbname은 코드 기본값을 쓰고, `DB_PORT`가 비어 있으면 `supabase/config.toml`의 로컬 포트를 사용한다. 값 오버라이드는 `.env -> os.environ` 순서를 따른다.
- **Legacy cycle 기본 machine:** 운영자가 `LEGACY_CYCLE_MACHINE_ID`를 비워 두지 않으면 `Cycle Ops` 화면 기본값으로 사용한다.

- **인증 전략:**

  - URL: `http://127.0.0.1:54321` (호스트에서 실행하는 로컬 Supabase 기준)

  - Key: `SUPABASE_ANON_KEY`

  - **필수 헤더:** 요청에 `Authorization`와 `apikey` 헤더를 모두 포함해야 한다.

- **i18n 리소스:** GUI에 노출되는 문구는 코드와 분리된 리소스 파일로 관리한다. 한국어를 기준 언어로 두고, 영어는 동일 키 구조로 대응한다. 배포본에서는 이 리소스가 PyInstaller 번들에 포함되어야 한다.

- **신규 기능 (v4.2):**
  - **스마트 동기화:** 업로드 전 서버 최신 데이터 타임스탬프를 확인해 중복을
    방지한다.
  - **병렬 업로드:** `ThreadPoolExecutor`로 동시 업로드(기본 4 스레드).
  - **실시간 진행률:** GUI에서 파일별 진행률과 전체 상태를 표시한다.
  - **업로드 범위:** 현재 Start Upload는 `PLC_DIR`만 사용하고 `TEMP_DIR`는 사용하지 않는다.
  - **custom 업로드 범위:** 단일 날짜가 아니라 `시작일 ~ 종료일` 포함 범위다.
  - **URL 표시:** GUI는 실제 설정 경로와 최종 업로드 URL을 로그에 표시한다.
  - **기본 Edge 경로:** `EDGE_FUNCTION_URL`이 비어 있으면 `SUPABASE_URL/functions/v1/upload-metrics`를 사용한다.
  - **수동 Edge override:** 필요하면 `EDGE_FUNCTION_URL`을 직접 지정할 수 있지만, 다른 호스트를 가리키면 잘못된 서버로 업로드될 수 있으므로 주의가 필요하다.
  - **보호 일치:** GUI는 수동 `EDGE_FUNCTION_URL`이 `SUPABASE_URL`과 다른 호스트를 가리키면 업로드를 시작하지 않는다.
  - **Smart Sync 문구:** `Smart Sync`는 서버 최신 timestamp 조회가 성공한 경우에만 필터를 적용한다.
  - **자동 업로드 문구:** `AUTO_UPLOAD=true`이면 앱 시작 5초 후 `Start Upload`를 자동 시도한다.
  - **Cycle Ops 분리:** cycle 정식 refresh, snapshot sync, parity/health, legacy backfill은 `Cycle Ops` 화면에서만 실행한다.
  - **Cycle snapshot 역할:** `tb_cycle_log`는 source of truth가 아니라 snapshot/compatibility sink로 취급한다.
- **문서/번들 전제:** GUI 문자열과 locale 자원은 분리하되, 배포본에서 읽을 수 있도록 `_MEIPASS` 기준 자원 해석과 spec 자원 포함을 전제로 한다.
- **Training 분리:** `training_base`, `training_dataset_v1`는 cycle snapshot과 직접 연결하지 않는다. 입력은 raw CSV lineage를 유지하지만, `die_id`, `billet_cycle_id`, cycle 관련 label 파생값은 integrated log 기준으로 포함될 수 있다.

## 4. 에지 함수 설정 (Deno)

- **파일:** `supabase/functions/upload-metrics/index.ts`

- **인증 전략 (환경 변수):**

  - **키 하드코딩 금지.** 코드는 `Deno.env.get('SUPABASE_ANON_KEY')`를 사용한다.

  - **URL:** `Deno.env.get('SUPABASE_URL')`를 사용한다.

- **로직:**
  - `POST`: 데이터 배치를 업로드한다.
  - `GET`: `timestamp` 기준 최신 타임스탬프를 반환한다(스마트 동기화에 사용).
  - 수동 헤더 검증(`getAuthToken`)은 제거되었다. Gateway 검증에 의존한다.

## 4-1. 주소 구분

- **호스트에서 GUI 실행:** `http://127.0.0.1:54321`
- **컨테이너 내부 통신:** `http://supabase_kong_Extrusion_data:8000`
- **커스텀 override:** `EDGE_FUNCTION_URL`에 직접 지정 가능하지만 기본값은 비워 두고 자동 계산을 사용하는 편이 안전하다.

## 5. 운영 절차 & 자동화

- **시스템 시작(재부팅 후):**

  1. **Windows (관리자 PowerShell):**
     `wsl --mount --vhd "E:\supabase-data.vhdx" --bare`

  2. **Ubuntu (터미널):**

     - `cd /mnt/c/Users/user/Documents/GitHub/Extrusion_data`

     - **`./startup.sh`** 실행 (프로젝트 루트 위치)
     - _이 스크립트는 프로젝트 디렉터리와 Docker 상태를 확인하고,
       Supabase/Grafana를 시작하며 Cron 서비스 실행을 보장하는 내부 운영 보조 스크립트다._
     - Windows GUI/배포본에서는 Dashboard의 `로컬 Supabase 시작` 버튼으로 같은 스크립트를 호출할 수 있다.
     - `Studio 열기` 버튼은 로컬 Studio 포트가 준비되면 `http://127.0.0.1:54323/`를 연다.
     - Dashboard에는 로컬 Supabase 상태 라벨과 시작/종료 진행 표시가 보인다.
     - `로컬 Supabase 종료` 버튼은 앱을 닫지 않고 로컬 스택만 종료한다.
     - 앱 종료 시 로컬 Supabase가 실행 중이면 함께 종료할지 확인한다.

- **백업 전략:**

  - 스크립트: `./supabase/auto/backup_daily.sh` (`.env`로 자격 증명 사용)
  - **범위:** `public` 스키마만.
  - **위치:**
    1. `.env`의 `BACKUP_DIR`가 있으면 해당 경로를 사용
    2. 없으면 기본값 `~/Documents/GitHub/Extrusion_data/backups`
    3. `.env`의 `BACKUP_MIRROR_DIR`가 있으면 추가 복사본을 남긴다
  - **자동화:** WSL Cron 작업이 매일 **18:30**에 실행하는 내부 운영 작업이다.

- **복구 전략:** `./restore_latest.sh`는 `.env`의 `BACKUP_DIR/latest.sql`을 우선 사용한다.

- **장기 보관 권장 경로:**
  - `.env`의 `ARCHIVE_DIR`
  - 권장 용도: 과거 원시 데이터의 `Parquet` 아카이브
  - 권장 위치: NAS 또는 별도 외장 저장소
  - 경로 형식은 실행 환경 기준으로 맞춘다. Windows Python이면 `Z:/...`, WSL이면 `/mnt/z/...`
  - GUI 경로: `Data Mgmt` 화면에서 archive 미리보기, export, 검증 후 삭제 흐름을 실행할 수 있다.

## 5-1. Cycle Operations

- **정식 실행 버튼:** `Cycle Ops -> Canonical Refresh`
  - `view_optimized_aligned_metrics`
  - `mv_work_log_effective_ranges`
  - `mv_optimized_metrics_work_log`
  - `mv_optimized_metrics_work_log_cache`
  를 순서대로 갱신한다.
- **Snapshot Sync:** canonical cache에서 cycle 경계를 집계해 `tb_cycle_log`에 upsert한다.
- **Parity / Health:** cache row 수, cycle 수, snapshot row 수, unmapped count, 최신 update 시각을 확인한다.
- **Legacy Backfill:** pressure threshold 기반 Python 로직으로 `tb_cycle_log`에 `source_mode='legacy_backfill'`로 적재한다.
- **운영 규칙:** 일반 사용자는 canonical 상태와 수동 갱신만 사용하고, 범위 기반 legacy backfill은 운영자 전용으로 제한한다.

## 6. 제약 및 금지 사항 (건드리지 말 것)

1. **코드에 비밀 정보 금지:** 소스 파일은 환경 변수(`.env`) 또는 Git에서
   무시되는 설정 파일을 사용해야 한다.

2. **컨테이너 내부에서 `localhost` 사용 금지:** 컨테이너 이름을 사용할 것
   (예: `supabase_db_Extrusion_data`).

3. **`~/supabase/docker` 금지:** 레거시 경로다. 작업은 프로젝트 루트에서만
   수행한다.

4. **`apikey` 헤더 제거 금지:** Python 클라이언트가 이 헤더 없이는 실패한다.
