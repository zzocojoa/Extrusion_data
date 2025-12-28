# Supabase 컨테이너 메모리 절감 우선순위

기준: 현재 메모리 사용량과 기능 의존도.  
목표: 업로더/DB/REST 중심 운영 시 불필요한 컨테이너부터 중지.

## 1순위 (가장 먼저 중지 고려)
- `supabase_analytics_Extrusion_data`  
  - 약 638 MiB 수준으로 가장 큼.
  - 로그/분석 기능을 쓰지 않으면 중지해도 핵심 업로드에는 영향 적음.

## 2순위 (상황에 따라 중지)
- `supabase_studio_Extrusion_data`  
  - 약 178 MiB.
  - Studio UI 미사용 시 중지 가능.  
- `supabase_realtime_Extrusion_data`  
  - 약 247 MiB.
  - Realtime 기능/Realtime 구독 미사용 시 중지 가능.

## 3순위 (대체 가능하지만 주의)
- `supabase_kong_Extrusion_data`  
  - 약 114 MiB.
  - API 게이트웨이. PostgREST 직접 호출로 대체 가능하지만 통합 URL 사용 시 영향 큼.
- `supabase_storage_Extrusion_data`  
  - 약 115 MiB.
  - Storage 미사용이면 중지 가능하나 파일 업로드/다운로드 연동 시 영향.

## 4순위 (가능하면 유지)
- `supabase_db_Extrusion_data`  
  - 약 154 MiB.
  - 필수. 중지하면 전체 서비스 중단.
- `supabase_rest_Extrusion_data`  
  - 약 20 MiB.
  - PostgREST API 핵심. 일반 업로드/쿼리에 필요.
- `supabase_auth_Extrusion_data`  
  - 약 15 MiB.
  - 인증 흐름 사용 시 필요. (익명 키만 사용하면 영향 낮음)
- `supabase_pg_meta_Extrusion_data`  
  - 약 98 MiB.
  - Studio/메타 API용. Studio 미사용 시 함께 중지 가능.
- `supabase_edge_runtime_Extrusion_data`  
  - 약 47 MiB.
  - Edge Functions 사용 시 필요.
- `supabase_inbucket_Extrusion_data`  
  - 약 20 MiB.
  - 메일 테스트 서버. 인증 메일 미사용 시 중지 가능.

## 비고
- 메모리 최적화만 목표라면 **analytics → studio → realtime** 순으로 줄이는 것이 가장 효과적입니다.
