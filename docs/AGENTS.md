# Repository Guidelines

## 프로젝트 구조 및 모듈 배치
- `core/`: 업로드, 변환, 사이클 처리, 상태 관리 핵심 로직(`upload.py`, `transform.py` 등).
- `uploader_gui_tk.py` / `uploader_cli.py`: GUI/CLI 진입점, 배포 빌드는 `dist/`에 생성.
- `supabase/`: 로컬 Supabase 스택(`config.toml`, `migrations/`, `functions/upload-metrics/` 엣지 함수).
- `tools/`: 데이터 점검·정렬 스크립트, 출력은 `tools/csv/` 또는 `backups/`에 보관.
- `assets/`: 아이콘·GUI 리소스, `build/`는 pyinstaller 산출물, `backups/`는 DB 덤프.

## 빌드·테스트·개발 명령어
- 가상환경: Windows `.\venv\Scripts\Activate.ps1`, WSL `source venv/bin/activate`.
- 로컬 실행: GUI `python uploader_gui_tk.py`, CLI `python uploader_cli.py`.
- 로컬 스택: 프로젝트 루트에서 `./startup.sh`(WSL) 또는 `supabase start`, 중지는 `supabase stop`.
- 패키징(GUI): `pyinstaller --onefile --noconsole --name ExtrusionUploader --icon assets\\app.ico uploader_gui_tk.py` (`ExtrusionUploader.spec` 참고).
- 패키징(CLI): `pyinstaller --onefile --noconsole --name ExtrusionUploaderCli --icon assets\\app.ico uploader_cli.py`.
- 백업/복원: `./supabase/auto/backup_daily.sh`, 복원 `./restore_latest.sh`.

## 코딩 스타일 및 네이밍
- Python 3.10+, 4칸 스페이스 들여쓰기, 타입 힌트 권장, 함수·변수는 snake_case 유지.
- Supabase 엣지 호출 시 `Authorization` + `apikey` 헤더를 항상 포함.
- 비밀값은 코드에 하드코딩 금지; `.env`/`config.ini`(git 무시)로 관리하고 `python-dotenv` 활용.
- 로그는 사용자 노출용이므로 간결한 한글 문구를 유지.

## 테스트 지침
- 공식 테스트 스위트는 없음; 병합 전 아래 점검 스크립트 우선 실행:
  - `python verify_integrated_etl.py` (ETL 일관성),
  - `python verify_view.py` (뷰/머터리얼라이즈드 뷰 검증),
  - `python tools/verify_alignment.py` 또는 `python tools/apply_alignment_shift.py` (정렬 확인).
- 마이그레이션·엣지 로직 수정 시 `supabase db reset`은 폐기 가능한 로컬 DB에서만 사용; 실데이터에는 금지.

## 커밋 & PR 가이드
- 커밋 메시지는 짧은 날짜형 요약 선호 예: `2025.12.23 08:37`; 한 커밋에 한 주제.
- PR에는 목적, 실행한 명령(빌드/테스트/백업), 관련 Supabase 마이그레이션 ID, GUI 변경 시 스크린샷/GIF를 포함.
- 이슈/작업과 연동하고, 설정·환경 변수 추가 시 `.env.example` 갱신 여부 명시.

## 보안 및 설정 유의사항
- `Project_Context.MD` 제약 준수: 컨테이너 내부에서 `localhost` 대신 컨테이너 이름 사용, `apikey` 헤더 유지, 레거시 경로 미변경.
- 실행 전 `.env`를 `.env.example`과 대조하고, 실 크리덴셜·백업 파일은 커밋 금지.
- USB/오프라인 백업 시 `/mnt/e` 마운트 확인 후 `backups/` 복사본을 검증.
