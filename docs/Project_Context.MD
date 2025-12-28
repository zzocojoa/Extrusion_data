# 프로젝트 컨텍스트 및 환경 요약 (Master v4.1 - 최종 최적화)

Supabase CLI를 사용해 알루미늄 압출 PLC 데이터용 로컬 데이터 분석 파이프라인을
구축 중이다.

**중요:** 이 프로젝트는 엄격한 네트워크/인증 규칙이 있는 특정 로컬 Docker
구성을 사용한다. 이 설정과 충돌하는 일반적인 해법을 제안하지 말 것.

## 1. 인프라 (로컬 하이브리드)

- **호스트 OS:** Windows 10/11 (WSL2 백엔드 활성화)

- **게스트 OS(서버):** Ubuntu (WSL2에서 실행)

- **물리 스토리지:**
  - **데이터베이스:** WSL 루트 파일시스템 (`ext4.vhdx`).
  - **백업 스토리지:** `E:\backups` (USB 드라이브).
    - **참고:** `E:\supabase-data.vhdx`는 WSL에 연결되어 있지만 실시간 DB
      저장소로는 사용하지 않는다. USB 백업용 마운트 지점으로만 사용한다.

## 2. 기술 스택 & 네트워크 (고정)

- **도구:** Supabase CLI (프로젝트 루트에서 실행:
  `/mnt/c/Users/user/Documents/GitHub/Extrusion_data`)

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

- **파일:** `core/upload.py`

- **설정 관리:** `config.ini` (Git 무시).

- **인증 전략:**

  - URL: `http://127.0.0.1:54321` (Kong Gateway)

  - Key: `config.ini`의 `sb_publishable_...` (Anon Key).

  - **필수 헤더:** Kong Gateway를 통과하려면 요청에 `apikey` 헤더(Anon Key와
    동일)가 반드시 포함되어야 한다.

- **신규 기능 (v4.2):**
  - **스마트 동기화:** 업로드 전 서버 최신 데이터 타임스탬프를 확인해 중복을
    방지한다.
  - **병렬 업로드:** `ThreadPoolExecutor`로 동시 업로드(기본 4 스레드).
  - **실시간 진행률:** GUI에서 파일별 진행률과 전체 상태를 표시한다.

## 4. 에지 함수 설정 (Deno)

- **파일:** `supabase/functions/upload-metrics/index.ts`

- **인증 전략 (환경 변수):**

  - **키 하드코딩 금지.** 코드는
    `Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')`를 사용한다.

  - **URL:** `Deno.env.get('SUPABASE_URL')`를 사용한다.

- **로직:**
  - `POST`: 데이터 배치를 업로드한다.
  - `GET`: `device_id` 기준 최신 타임스탬프를 반환한다(스마트 동기화에 사용).
  - 수동 헤더 검증(`getAuthToken`)은 제거되었다. Gateway 검증에 의존한다.

## 5. 운영 절차 & 자동화

- **시스템 시작(재부팅 후):**

  1. **Windows (관리자 PowerShell):**
     `wsl --mount --vhd "E:\supabase-data.vhdx" --bare`

  2. **Ubuntu (터미널):**

     - `cd /mnt/c/Users/user/Documents/GitHub/Extrusion_data`

     - **`./startup.sh`** 실행 (프로젝트 루트 위치)
     - _이 스크립트는 프로젝트 디렉터리와 Docker 상태를 확인하고,
       Supabase/Grafana를 시작하며 Cron 서비스 실행을 보장한다._

- **백업 전략:**

  - 스크립트: `./supabase/auto/backup_daily.sh` (`.env`로 자격 증명 사용)
  - **범위:** `public` 스키마만.
  - **위치:**
    1. 로컬: `~/Documents/GitHub/Extrusion_data/backups`
    2. USB: `/mnt/e/backups` (마운트된 경우)
  - **자동화:** WSL Cron 작업이 매일 **18:30**에 실행.

- **복구 전략:** `./restore_latest.sh`

## 6. 제약 및 금지 사항 (건드리지 말 것)

1. **코드에 비밀 정보 금지:** 소스 파일은 환경 변수(`.env`) 또는 Git에서
   무시되는 설정 파일을 사용해야 한다.

2. **컨테이너 내부에서 `localhost` 사용 금지:** 컨테이너 이름을 사용할 것
   (예: `supabase_db_Extrusion_data`).

3. **`~/supabase/docker` 금지:** 레거시 경로다. 작업은 프로젝트 루트에서만
   수행한다.

4. **`apikey` 헤더 제거 금지:** Python 클라이언트가 이 헤더 없이는 실패한다.
