# 저장소 정리 및 구조 개편 제안

## 현황 요약
- 소스: `core/`와 루트의 GUI/CLI 스크립트(`uploader_gui_tk.py`, `uploader_cli.py`), `supabase/` 마이그레이션·엣지 함수, `tools/` 점검 스크립트.
- 배포 산출물: `build/`, `dist/`에 pyinstaller 결과 존재; `.gitignore`에 포함되지만 과거 커밋에 남아 있을 수 있음.
- 데이터: `backups/`, `grafana_data/`, `tools/csv/`, `압출일보*.csv/xlsm` 등 대용량 원본. `.gitignore`에 있으나 일부는 Git에 추적된 상태로 보관될 가능성.
- 로그/임시: `alignment_log.txt`, `error.log`, `__pycache__/` 등 실행 부산물.

## 즉시 정리/제외 대상
- Git 추적 해제 후 로컬 보존: `build/`, `dist/`, `grafana_data/`, `tools/`, `backups/` → `git rm --cached -r build dist grafana_data tools` 실행 후 `.gitignore` 유지.
- 가상환경/임시: `venv/`, `__pycache__/`, `*.pyc`, `.env`, `config.ini` → 이미 `.gitignore`에 있으나 추적 중이면 `git rm --cached` 처리.
- 대용량 원본: `압출일보*.csv/xlsm` → `data/raw/`(gitignore)로 이동.
- 로그: `alignment_log.txt`, `error.log` 등 → `logs/`(gitignore)로 이동, 필요 시 로테이션 스크립트 추가.

## 권장 폴더 구조(정리 후)
- `core/` + `uploader_gui_tk.py`/`uploader_cli.py`: 애플리케이션 코드.
- `supabase/`: `config.toml`, `migrations/`, `functions/` 등 Supabase 자산.
- `scripts/`: 운영/자동화 스크립트(autorun, 백업 호출 래퍼 등).
- `tools/`: 데이터 점검 유틸; 출력물은 `data/` 또는 `logs/`로 분리.
- `docs/`: `Project_Context.MD`, `Project_Analysis_Report.md`, `AGENTS.md` 등 문서.
- `data/`(gitignore): `backups/`, `raw/` 원본 데이터, `grafana_data/`, `tools/csv/` 등 로컬 저장소.
- `build/`, `dist/`: 로컬 빌드 산출물(항상 gitignore).

## 실행 단계
1) `mkdir -p data/raw data/backups data/grafana logs docs` 등 디렉터리 준비 후 데이터·로그 이동.
2) `git rm --cached -r build dist grafana_data tools`로 이미 추적된 산출물/데이터를 Git에서 분리.
3) 압출일보 원본과 백업 SQL을 `data/raw` 또는 `data/backups`로 이동; `tools/csv`도 `data/`로 이동.
4) 문서류(`Project_Context.MD`, `Project_Analysis_Report.md`, `AGENTS.md`)를 `docs/`로 이동하고 경로를 README/운영 문서에 반영.
5) `.gitignore`에 `data/`, `logs/`가 포함됐는지 확인(중복 정리 포함) 후 커밋.

## 주의사항
- `supabase/migrations/`, `supabase/functions/` 등 재현 불가한 자산은 절대 삭제하지 않음.
- `Project_Context.MD` 제약 준수: 컨테이너 내부에서는 컨테이너 이름 사용, `apikey` 헤더 삭제 금지, 레거시 경로 미수정.
- 정리 후 `python uploader_gui_tk.py` 등 기본 실행과 `python verify_view.py` 같은 검증 스크립트를 돌려 경로 문제를 확인.
