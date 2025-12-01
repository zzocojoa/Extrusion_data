-- 1. "all_metrics" 라는 이름의 테이블(선반)을 만듭니다.
CREATE TABLE public.all_metrics (
    -- 'timestamp' 칸: 타임존 포함된 시간(TIMESTAMPTZ) 타입, 절대 비어있으면 안됨(NOT NULL)
    "timestamp" TIMESTAMPTZ NOT NULL,

    -- 'device_id' 칸: 어떤 장비의 데이터인지 식별하는 ID. 문자열(TEXT) 타입, 절대 비어있으면 안됨(NOT NULL)
    device_id TEXT NOT NULL,

    -- 'temperature' 칸: 온도 데이터 (섭씨)
    temperature DOUBLE PRECISION NULL,

    -- 'main_pressure' 칸: PLC 메인 압력
    main_pressure DOUBLE PRECISION NULL,

    -- 'billet_length' 칸: PLC 빌렛 길이
    billet_length DOUBLE PRECISION NULL,

    -- 'container_temp_front' 칸: PLC 콘테이너 온도 (앞쪽)
    container_temp_front DOUBLE PRECISION NULL,

    -- 'container_temp_rear' 칸: PLC 콘테이너 온도 (뒷쪽)
    container_temp_rear DOUBLE PRECISION NULL,

    -- 'production_counter' 칸: PLC 생산 카운터
    production_counter BIGINT NULL,

    -- 'current_speed' 칸: PLC 현재 속도
    current_speed DOUBLE PRECISION NULL
);

-- 2. "timestamp"와 "device_id" 조합이 중복되지 않도록 고유 제약(UNIQUE)을 겁니다.
-- 이것이 중복 업로드를 막아주는 핵심 규칙입니다.
ALTER TABLE public.all_metrics
ADD CONSTRAINT all_metrics_timestamp_device_id_key
UNIQUE ("timestamp", device_id);

-- TimescaleDB 관련 코드는 PostgreSQL 17과 호환성 문제로 제거되었습니다.
