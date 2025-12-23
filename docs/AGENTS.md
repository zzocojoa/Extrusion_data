# Repository Guidelines

## 프로젝트 구조 및 모듈 배치
- `core/`: 업로드/변환/상태/파일 처리 핵심 로직(`upload.py`, `transform.py` 등).
- `uploader_gui_tk.py` / `uploader_cli.py`: GUI/CLI 진입점, 배포 빌드는 `dist/`에 생성.
- `supabase/`: 로컬 Supabase 스택(`config.toml`, `migrations/`, `functions/upload-metrics/` 엣지 함수).
- `tools/`: 데이터 점검·정렬 스크립트, 출력은 `data/` 또는 `logs/`로 분리.
- `assets/`: 아이콘·GUI 리소스, `build/`는 pyinstaller 산출물, `data/backups/`는 DB 덤프.

## 빌드·테스트·개발 명령어
- 가상환경: Windows `.\venv\Scripts\Activate.ps1`, WSL `source venv/bin/activate`.
- 로컬 실행: GUI `python uploader_gui_tk.py`, CLI `python uploader_cli.py --quick`.
- 빌드: GUI `scripts\build_gui.ps1` 또는 `bash scripts/build_gui.sh`, CLI `scripts\build_cli.ps1` 또는 `bash scripts/build_cli.sh` (산출물은 `dist/`).
- 백업/복원: `./supabase/auto/backup_daily.sh`, 복원 `./restore_latest.sh`.

## 코딩 스타일 및 네이밍
- Python 3.10+, 4칸 스페이스, 타입 힌트 권장, snake_case 유지.
- Supabase 호출 시 `Authorization` + `apikey` 헤더 필수.
- 비밀값은 `.env`/`config.ini`(gitignore)로 관리하고 `python-dotenv` 활용.
- 로그 메시지는 사용자 노출 기준으로 간결하게 유지.

## 테스트 지침
- 검증 스크립트:  
  - ETL: `python tools/verify_integrated_etl.py --file <CSV> --filename-hint 251209`  
  - 뷰/API: `python tools/verify_view.py --sample-limit 1`  
  종료 코드: 0=성공, 1=실패, 2=설정 누락.
- 마이그레이션/엣지 수정 시 실DB에 `supabase db reset` 금지, 필요 시 로컬 disposable DB에서만 실행.

## 커밋 & PR 가이드
- 메시지 예: `2025.12.23 08:37` 형태의 짧은 일자 요약, 커밋당 한 주제.
- PR에는 목적, 실행한 명령(빌드/테스트/백업), 관련 Supabase 마이그레이션 ID, GUI 변경 시 스크린샷/GIF 포함.
- 설정/환경 변경 시 `.env.example` 업데이트 여부를 명시.

## 보안 및 설정 유의사항
- `Project_Context.MD` 제약 준수: 컨테이너 내부에서는 `localhost` 대신 컨테이너 이름, `apikey` 헤더 유지, 레거시 경로 미수정.
- `.env`를 `.env.example`과 대조 후 실행, 실 크리덴셜/백업은 커밋 금지.
- USB/오프라인 백업 시 `/mnt/e` 마운트와 `data/backups/` 복사본을 확인.
