# Extrusion Data Uploader

Extrusion Uploader는 GUI 중심 앱입니다. 사용자는 `ExtrusionUploader.exe`를 실행하거나 `python uploader_gui_tk.py`로 GUI를 실행하면 됩니다. GUI는 `%APPDATA%\ExtrusionUploader\config.ini`를 기본 설정으로 읽고, 개발 실행에서는 저장소 루트 `.env`, 배포본에서는 실행 파일 옆 `.env`, 그리고 프로세스 환경변수로 값을 override할 수 있습니다. UI 언어는 `UI_LANGUAGE` 설정으로 관리하며, `ko`와 `en`을 지원하는 i18n 리소스는 배포본에 함께 포함되는 것을 전제로 합니다.

## 빠른 시작
- 가상환경 활성화
  - macOS/WSL: `source venv/bin/activate`
  - Windows: `.\venv\Scripts\Activate.ps1`
- 설정: `SUPABASE_URL`, `SUPABASE_ANON_KEY`, 필요 시 `EDGE_FUNCTION_URL` 입력. 로컬 기본값은 `SUPABASE_URL=http://127.0.0.1:54321` 입니다. UI 언어를 바꾸려면 `UI_LANGUAGE=ko|en`을 사용합니다.
- 첫 실행 시 루트 `config.ini`가 `%APPDATA%\ExtrusionUploader\config.ini`로 복사됩니다. 이후 GUI 저장은 AppData 쪽 파일에 반영되고, `.env`와 프로세스 환경변수는 그 값을 override합니다.

## 실제 적용 설정
- 가장 먼저 확인할 파일: `%APPDATA%\ExtrusionUploader\config.ini`
- 첫 실행 전 기본 예시: 저장소 루트 `config.ini`
- 개발 실행 override: 저장소 루트 `.env`
- 배포본 override: `ExtrusionUploader.exe`와 같은 폴더의 `.env`
- 최종 override: `os.environ`
- UI 언어는 `UI_LANGUAGE`가 있으면 그것을 우선하고, 없으면 기본값 `ko`를 사용합니다.
- Settings의 `비우기`는 `EDGE_FUNCTION_URL` 입력값만 지우며, `설정 저장` 후 자동 계산값이 적용됩니다.
- Settings의 `업로드 범위=custom`은 단일 날짜가 아니라 `시작일 ~ 종료일` 포함 범위를 사용합니다.
- GUI는 시작 시 실제 설정 경로와 최종 업로드 URL을 로그에 표시합니다.
- i18n 문자열은 UI용 리소스로 분리해 관리하며, 배포본에서는 locale 파일이 exe와 함께 제공되는 구성을 전제로 합니다.

적용 우선순위는 다음과 같습니다.
- 기본값
- `%APPDATA%\ExtrusionUploader\config.ini`
- `.env`
- `os.environ`

## 저장 위치 요약
| 구분 | 실제 위치 | 설명 |
| --- | --- | --- |
| 업로드된 측정 데이터 | 로컬 Supabase `public.all_metrics` | 물리적으로는 WSL 쪽 DB에 저장됩니다. |
| 원본 CSV | `PLC_DIR` | 현재 `Start Upload`는 이 폴더만 읽습니다. |
| 실제 지속 설정 | `%APPDATA%\ExtrusionUploader\config.ini` | GUI 저장도 이 파일에 반영됩니다. |
| 업로드 이력 | `%APPDATA%\ExtrusionUploader\processed_files.log` | 이미 처리한 파일 기록입니다. |
| 업로드 재개 상태 | `%APPDATA%\ExtrusionUploader\upload_resume.json` | 파일별 재개 오프셋입니다. |
| 프로젝트 백업 | `BACKUP_DIR` 또는 기본 `backups/` | 실시간 DB와 별개인 SQL 백업입니다. |
| 장기 보관 아카이브 | `ARCHIVE_DIR` | `public.all_metrics`를 Parquet로 내보내는 경로입니다. |

NAS를 사용할 경우:
- SQL 백업 경로는 `.env`의 `BACKUP_DIR`로 지정합니다.
- 장기 보관 아카이브 경로는 `.env`의 `ARCHIVE_DIR`로 분리하는 것을 권장합니다.
- 실시간 DB 본체는 NAS가 아니라 WSL/Docker 쪽에 그대로 두는 것이 안전합니다.
- `ARCHIVE_DIR`는 GUI `Data Mgmt`에서 사용하는 경로 기준으로 넣어야 합니다. Windows Python이면 `Z:/...`, WSL이면 `/mnt/z/...` 형식이 맞습니다.

## 로컬 실행
- GUI: `python uploader_gui_tk.py`
- GUI `Data Mgmt` 화면에서 아카이브 `dry-run` / export / 검증 후 삭제를 실행할 수 있습니다.
- 배포본: `ExtrusionUploader.exe`
- `ExtrusionUploader.exe`는 Dashboard의 `로컬 Supabase 시작` 버튼으로 WSL `startup.sh`를 실행할 수 있고, `Studio 열기` 버튼으로 `http://127.0.0.1:54323/`를 열 수 있습니다.
- Dashboard에는 로컬 Supabase 상태 라벨과 시작/종료 중 진행 표시가 보이며, `로컬 Supabase 종료` 버튼으로 로컬 스택만 내릴 수 있습니다.
- Dashboard에는 WSL 루트 파일시스템 저장공간 카드가 함께 표시되며, `사용 중`/`여유 공간`/`사용률`과 위험 상태를 한눈에 확인할 수 있습니다.
- host 쪽 `ext4.vhdx` 크기와 드라이브 여유 공간은 `WSL_VHDX_PATH`가 설정된 경우에만 함께 표시되며, 설정이 없으면 partial 상태로 명시됩니다.
- 앱 종료 시 로컬 Supabase가 실행 중이면 함께 종료할지 확인합니다.
- Settings의 `Smart Sync`는 서버 최신 timestamp 조회가 성공한 경우에만 필터를 적용합니다.
- Settings의 `앱 시작 5초 후 자동 업로드 시도`는 앱 시작 후 `Start Upload`를 자동으로 시도합니다.
- Settings의 `custom` 범위는 시작일/종료일 엔트리와 커스텀 달력 팝업으로 설정합니다.

## 업로드/재개 로직
- `core.upload.upload_item`을 사용하며 `Authorization` + `apikey` 헤더에 `SUPABASE_ANON_KEY`를 사용합니다.
- 재시도/재개 상태: `upload_resume.json`, 처리 로그: `processed_files.log` (모두 AppData 하위 관리).
- `Start Upload`는 현재 `PLC_DIR`만 사용합니다.
- `EDGE_FUNCTION_URL`이 비어 있으면 `SUPABASE_URL/functions/v1/upload-metrics`로 자동 계산합니다.
- `RANGE_MODE=custom`이면 `CUSTOM_DATE_START`, `CUSTOM_DATE_END`를 함께 사용하고, 해당 기간의 파일만 포함합니다.
- GUI는 수동 `EDGE_FUNCTION_URL`이 `SUPABASE_URL`과 다른 호스트를 가리키면 업로드를 시작하지 않습니다.
- `UI_LANGUAGE`는 AppData `config.ini`를 기준으로 저장되며, 배포본에서도 동일한 설정 우선순위를 유지합니다.

## 주요 운영 기능
- GUI `Start Upload`는 `PLC_DIR`의 원본 CSV를 읽어 업로드합니다.
- GUI Dashboard는 로컬 Supabase runtime 상태와 함께 WSL 저장공간 상태를 시각적으로 표시합니다.
- GUI `Data Mgmt`는 아카이브 `dry-run` / export / 검증 후 삭제를 제공합니다.
- GUI `Data Mgmt`의 아카이브 흐름은 `.env`의 `DB_PASSWORD`, `ARCHIVE_DIR`를 사용하며, `before_date=YYYY-MM-DD` 폴더 아래 Parquet를 생성합니다.
- 아카이브 미리보기는 실제 export/delete 없이 대상 row 수, timestamp 범위, 출력 경로, DB 대상을 먼저 보여줍니다.
- 검증 후 삭제는 Parquet 재검증 이후에만 동작합니다.

## 내부 스크립트
- 저장소에는 백업, 복원, 검증, 빌드용 보조 스크립트가 있습니다.
- 이 스크립트들은 내부 운영과 유지보수용이며, 일반 사용자는 GUI 또는 `ExtrusionUploader.exe`만 사용하면 됩니다.

## 주의사항
- 호스트에서 GUI를 실행할 때는 `http://127.0.0.1:54321`을 기준으로 설정하세요.
- 컨테이너 내부에서는 `localhost` 대신 컨테이너 이름을 사용하세요. 예: `http://supabase_kong_Extrusion_data:8000`
- `EDGE_FUNCTION_URL`은 기본적으로 비워 두는 편이 안전합니다. 비워 두면 `SUPABASE_URL` 기준으로 자동 계산됩니다.
- 커스텀 `EDGE_FUNCTION_URL` override는 가능하지만, 다른 호스트를 가리키면 잘못된 서버로 업로드될 수 있으니 주의가 필요합니다.
- `WSL_VHDX_PATH`를 AppData `config.ini`, `.env`, 또는 `os.environ`에 넣어야 Dashboard가 host 쪽 `ext4.vhdx` 파일 크기와 드라이브 여유 공간을 그 경로 기준으로 표시합니다.
- `apikey` 헤더 제거 금지.
- 실제 크리덴셜/백업은 커밋 금지; `data/`, `logs/`는 gitignore 대상.
- 배포본의 UI 문자열은 locale 리소스 파일을 기준으로 표시되므로, exe 배포 시 i18n 자원이 함께 포함되어야 합니다.
