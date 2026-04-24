# 구축 반영 여부 정리 (docs/ 기준)

docs/ 폴더의 문서 내용과 현재 저장소 구성을 대조해 반영 여부를 정리했다.

## 반영됨
- Supabase 로컬 설정/포트/CLI 실행 흐름: `supabase/config.toml`, `startup.sh`
- 업로더 공통 로직(재시도·resume·Smart Sync)과 `apikey` 헤더: `core/upload.py`, `core/state.py`
- GUI 병렬 업로드(기본 4)와 파일별 진행률: `uploader_gui_tk.py`
- GUI가 core 공통 모듈 재사용 + 설정 검증 적용: `uploader_gui_tk.py`, `core/transform.py`, `core/config.py`, `core/upload.py`
- config.ini Git-ignored 및 경로 관리: `core/config.py`, `.gitignore`
- 백업/검증 스크립트 제공: `supabase/auto/backup_daily.sh`, `tools/verify_integrated_etl.py`, `tools/verify_view.py`

## 미반영/불일치
- Edge Function 인증/환경 변수: 문서의 `SUPABASE_SERVICE_ROLE_KEY`와 다르게 실제는 `SUPABASE_ANON_KEY` 사용 + URL fallback 하드코딩 존재: `supabase/functions/upload-metrics/index.ts`
- Smart Sync의 `device_id` 기반 최신 타임스탬프 조회가 문서와 다름(서버는 필터 제거): `core/upload.py`, `supabase/functions/upload-metrics/index.ts`
- 온도 센서 데이터 수집은 변환 함수가 있으나 후보 파일 수집에서 로직이 제거되어 실제 업로드 경로가 끊김: `core/transform.py`, `core/files.py`
- 공정 세분화 스크립트: 문서의 `run_segmentation.py`는 없고 로직은 다른 파일에 존재: `core/cycle_processing.py`
- “코드에 비밀정보 금지/컨테이너 localhost 금지” 제약과 충돌(하드코딩 DB 자격증명·URL): `core/cycle_processing.py`, `restore_latest.sh`, `supabase/auto/backup_daily.sh`
- 리팩터링 문서의 검증/로그 표준화 일부 미반영(구조화 로그 포맷 없음, `scripts` 하위 검증 경로 부재): `uploader_gui_tk.py`, `scripts`

## 추가 확인 필요
- Docker 네트워크 이름 및 Grafana 연결 설정은 런타임/컨테이너 설정에 좌우되어 저장소만으로 확정하기 어려움.

## 기준 문서
- `docs/Project_Context.MD`
- `docs/Project_Analysis_Report.md`
- `docs/REFACTORING_OBJECTIVES.md`
- `docs/REFACTORING_STEPS.md`
- `docs/AGENTS.md`
