# 리팩토링 단계 제안 (실무용)

## 설계·기초 정리
- docs/REFACTORING_OBJECTIVES.md 기준 모듈 다이어그램: core(업로드/변환/상태/파일), UI(GUI), infra(Supabase REST/Edge), 검증/테스트, 빌드.
- 공통 설정 계약 정의: 필수/선택 환경키, 기본값, 실패 시 메시지 표준.

## 구조 정리 1: 공통 업로더 경로
- `core.upload.upload_item` 인터페이스 고정(헤더, 재시도, resume, batch, progress).
- GUI 업로드 호출 경로를 공통 헬퍼로 정리(`core/upload_runner.py` 등).
- 로그/상태 파일 경로를 `get_data_dir()` 단일 출처로 통일.

## 구조 정리 2: 설정/입력 검증
- `core.config`에 스키마(필수/선택)와 검증 함수 추가, 실패 시 명확 메시지.
- 파일 후보 선정(`core.files`)에 옵션 객체 도입: cutoff, lag, lock 체크, quick 모드.

## 코드 중복 제거
- GUI 입력 처리 중복 제거, `core.transform`만 사용.
- GUI 업로드 호출도 공통 함수 사용으로 중복 삭제.
- 로그 포맷(성공/실패/재시도) 상수화.

## 관측성·로깅 정비
- `logs/` 아래 구조화 로그(ISO 시간, level, event, context) 기록.
- 오류: 파일에는 상세, 사용자 메시지는 축약. 성공/실패 포맷 일관화.
- 검증 스크립트(`tools/verify_*`, `scripts/verification/*`) 출력 포맷 정렬.

## 테스트 진입점 마련
- 샘플 데이터 기반 스모크: 변환(`core.transform`)과 업로드 모의(HTTP mock/dry-run).
- 검증 스크립트 인터페이스 정리: `python tools/verify_view.py --config ...` 등.
- 단위 테스트 추가: 파일 후보 선정, resume set/get, 헤더 포함 여부 등.

## 빌드/배포 단순화
- `ExtrusionUploader.spec` 정리: 데이터 디렉터리, 아이콘, 콘솔 옵션 확인.
- 빌드 명령 래퍼: `scripts/build_gui.(sh|ps1)`.
- `.env.example`·`README`·`AGENTS`에 빌드/실행/테스트 명령 동기화.

## 이행 순서(추천)
1) 설계/계약 문서화 → 모듈 다이어그램.
2) 공통 업로더 경로 정리 + GUI 적용, 중복 입력 처리 제거.
3) 설정 검증·파일 후보 옵션 객체화, 로그 포맷 통일.
4) 검증 스크립트 인터페이스 정리, 스모크 테스트 추가.
5) pyinstaller 빌드 래퍼 정리 및 문서 동기화.
6) 회귀 확인(샘플 데이터 업로드·뷰 검증), CI/체크리스트 작성.

## 성공 체크
- 코드 중복 감소(공통 모듈 재사용), 환경 누락 시 즉시 실패, 로그/검증 출력 일관.
- 빌드·테스트가 1~2 커맨드로 완료, 오류 메시지가 명확.
