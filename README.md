# Extrusion Data Uploader

로컬 Supabase 스택을 통해 압출 공정 데이터를 업로드/검증합니다. GUI/CLI 모두 `.env` 또는 `config.ini` 설정이 필요하며, `.env.example`를 참고해 환경 변수를 준비하세요.

## 빠른 시작
- 가상환경 활성화
  - macOS/WSL: `source venv/bin/activate`
  - Windows: `.\venv\Scripts\Activate.ps1`
- 설정: `.env` 또는 `config.ini`에 `SUPABASE_URL`, `SUPABASE_ANON_KEY`, 필요 시 `EDGE_FUNCTION_URL` 입력.

## 로컬 실행
- GUI: `python uploader_gui_tk.py`
- CLI(빠른 후보 탐색): `python uploader_cli.py --quick`

## 업로드/재개 로직
- `core.upload.upload_item`을 사용하며 `Authorization` + `apikey` 헤더 포함.
- 재시도/재개 상태: `upload_resume.json`, 처리 로그: `processed_files.log` (모두 AppData 하위 관리).

## 빌드
- GUI 빌드: `scripts\build_gui.ps1` (PowerShell) / `bash scripts/build_gui.sh`
- CLI 빌드: `scripts\build_cli.ps1` (PowerShell) / `bash scripts/build_cli.sh`
- 산출물: dist/ 아래 생성.

## 검증 스크립트
- ETL 샘플 검증: `python tools/verify_integrated_etl.py --file <CSV> --filename-hint 251209`
- 뷰/API 검증: `python tools/verify_view.py --sample-limit 1`
- 종료 코드: 0=성공, 1=실패, 2=설정 누락.

## 백업/운영
- Supabase 백업: `./supabase/auto/backup_daily.sh`
- 최신 백업 복원: `./restore_latest.sh`
- SQL 편집 저장: `./supabase/auto/save_gui_changes.sh`

## 주의사항
- 컨테이너 내부에서는 `localhost` 대신 컨테이너 이름 사용.
- `apikey` 헤더 제거 금지.
- 실제 크리덴셜/백업은 커밋 금지; `data/`, `logs/`는 gitignore 대상.
