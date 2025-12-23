# 프로젝트 분석 보고서: 알루미늄 압출 공정 데이터 파이프라인 (Extrusion Data Pipeline)

## 0. 개요

본 프로젝트는 알루미늄 압출 공장에서 발생하는 **PLC 데이터(압력, 속도 등)와 센서
데이터(온도)**를 수집, 통합, 분석하여 시각화하는 **Smart Factory 데이터
파이프라인 시스템**입니다. 단순한 데이터 수집을 넘어, 원시(Raw) 시계열 데이터를
**'생산 주기(Cycle/Billet)' 단위로 세분화(Segmentation)**하여 품질 관리와 공정
최적화에 필요한 핵심 지표를 제공하는 것을 목적으로 합니다.

---

## 1. 기술 아키텍처 (Technical Architecture)

공장 현장의 특수성(네트워크 제약, 신뢰성 요구)을 고려하여 **Hybrid Local Cloud**
형태의 아키텍처를 채택하고 있습니다.

### 1.1 인프라 (Local Hybrid)

- **Host**: Windows 10/11 (사용자/운영자 환경)
- **Server**: WSL2 Ubuntu + Docker Containers (데이터베이스 및 백엔드)
- **특징**: 외부 클라우드 의존성 없이 공장 로컬 망 내에서 완결되는 구조로,
  보안성과 데이터 주권을 확보하면서도 클라우드 네이티브 기술(Docker, Supabase)의
  장점을 활용함.

### 1.2 핵심 스택 (Tech Stack)

- **Backend & DB**: **Supabase (Self-hosted on Docker)**
  - PostgreSQL을 핵심 저장소로 사용하며, PostgREST를 통해 별도의 API 서버 개발
    없이 CRUD 인터페이스 확보.
  - Edge Functions (Deno): 데이터 업로드 시 중복 방지(Deduplication) 및 유효성
    검사 로직 수행.
- **Client (Uploader)**: **Python (Tkinter/CustomTkinter)**
  - Windows 실행 파일(.exe)로 배포되어 운영자가 쉽게 실행.
  - 폴더 감시(Watchdog) 및 파일 파싱(Pandas)을 수행하여 로컬 파일 시스템의 PLC
    로그를 DB로 전송.
- **Visualization**: **Grafana**
  - PostgreSQL에 직접 연결하여 실시간 차트 및 대시보드 제공.

---

## 2. 핵심 비즈니스 로직 (Core Business Logic)

### 2.1 데이터 수집 및 스마트 동기화 (Smart Ingestion)

- **다중 소스 통합**: 이종 기기(Extruder PLC, Spot Thermometer)에서 생성되는
  서로 다른 포맷(CSV, Excel)의 통합 처리.
- **스마트 싱크 (Smart Sync)**: `uploader_gui_tk.py`와 Edge Function이 협력하여,
  이미 서버에 저장된 데이터 시점 이후의 데이터만 선별적으로 업로드. (네트워크
  대역폭 절약 및 중복 방지)
- **안정성 확보**: 파일 잠금(Lock) 감지, 재시도 로직, 로컬 상태 저장(Resume
  Capability)을 통해 공장 환경의 불안정한 I/O에 대응.

### 2.2 공정 세분화 (Cycle Segmentation) (`run_segmentation.py`)

이 프로젝트의 가장 핵심적인 부가가치를 창출하는 로직입니다.

- 단순 시계열 나열인 `all_metrics` 데이터를 분석하여 의미 있는 **'작업
  단위(Cycle)'**로 변환.
- **알고리즘**:
  - **압력 임계값(Pressure Threshold > 30 bar)**을 기준으로 압출 시작/종료 자동
    감지.
  - 최소 지속 시간(Duration) 및 최대 압력(Max Pressure) 필터를 통해 테스트
    런이나 노이즈 제거 (`is_valid` 판별).
  - 생산 카운터(Production Counter)와 매핑하여 각 빌렛(Billet) 별 생산 이력
    생성.

---

## 3. 개발자 관점의 분석 (Technical Review)

### 3.1 강점 (Pros)

- **실용적인 "Low Code" 접근**: Django/Spring 같은 Heavy한 백엔드 프레임워크
  대신, Supabase와 Grafana를 활용하여 **"Time-to-Market"**을 극대화함. 핵심
  로직(수집, 분석)에만 집중할 수 있는 구조.
- **현장 친화적 클라이언트**: 웹 브라우저 기반 업로드보다 안정적인 Python
  Desktop App 방식을 택하여, 장시간 구동이 필요한 공장 PC 환경에 적합함.
- **데이터 무결성 고려**: 중복 업로드 방지 로직과 데이터 검증(Validation) 단계가
  파이프라인 곳곳에 배치되어 있어 데이터 신뢰도가 높음.

### 3.2 개선 제안 및 고려사항 (Considerations)

- **확장성 (Scalability)**: 현재 `run_segmentation.py`가 전체 데이터를
  조회(`fetch_metrics`)하여 메모리에서 처리하는 방식은 추후 데이터가 수백만 건
  이상 쌓일 경우 성능 병목이 될 수 있음.
  - _제안: 마지막 분석 시점 이후 데이터만 증분 처리(Incremental
    Processing)하도록 로직 고도화 필요._ (현재도 부분적으로 적용된 것으로
    보이나, 확실한 Cursor 기반 처리가 권장됨)
- **시간 동기화 (Time Sync)**: PLC, 센서, 서버(WSL), 클라이언트(Windows) 간의
  시간대(KST vs UTC) 불일치가 발생할 경우 데이터 정합성이 깨질 수 있음. 모든
  타임스탬프를 UTC로 통일하고 View Layer에서만 KST로 변환하는 원칙 엄수가
  중요함.
- **유지보수 (Maintenance)**: 로컬 Docker 인프라(볼륨 관리, 백업 등)에 대한 운영
  의존도가 높음. 문제 발생 시 현장 대응이 어려울 수 있으므로, 원격 모니터링이나
  자동 복구 스크립트(`startup.sh` 등)의 지속적인 관리가 필수적임.

## 4. 결론

이 프로젝트는 **"현장 데이터의 디지털 자산화"**를 성공적으로 수행하기 위한
견고한 기반을 갖추고 있습니다. 특히 물리적인 제조 공정을 소프트웨어적인 논리
단위(Cycle)로 변환하는 로직은 향후 **예지 보전(Predictive Maintenance)**이나
**품질 자동 판정(AI Quality Control)**으로 나아가기 위한 핵심 자산이 될
것입니다.
