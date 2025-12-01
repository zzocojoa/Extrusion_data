-- 1. 트랜잭션 시작 (안전하게 처리)
BEGIN;

-- 2. 기존 뷰 삭제 (깨끗한 상태로 시작)
DROP VIEW IF EXISTS public.view_ml_learning_data;

-- 3. 뷰 재생성 (최신 로직: 컬럼명, 시간/단위 보정 완벽 적용)
CREATE OR REPLACE VIEW public.view_ml_learning_data AS
SELECT
    m."timestamp",
    m.device_id,
    m.main_pressure,
    m.current_speed,
    m.billet_length,
    m.temperature AS actual_exit_temp, -- [수정됨] 실제 출구 온도
    m.container_temp_front,
    m.container_temp_rear,
    m.extrusion_end_position,

    w.machine_id,
    w.die_id,
    w.product_name,
    w.alloy_type,
    w.worker_name,
    w.target_billet_temp,
    w.target_exit_temp,
    w.production_qty,

    -- [데이터 보정] 단위: kg (1000 나누기)
    CASE 
        WHEN w.production_weight > 100000 THEN w.production_weight / 1000.0 
        ELSE w.production_weight 
    END AS production_weight,
    
    CASE 
        WHEN w.productivity > 100000 THEN w.productivity / 1000.0 
        ELSE w.productivity 
    END AS productivity,

    -- [시간 보정] UTC 변환 (-9시간) 및 기계 가동 시간 우선 적용
    COALESCE(w.corrected_start_time, w.start_time - INTERVAL '9 hours') AS start_time,
    (w.end_time - INTERVAL '9 hours') AS end_time

FROM
    public.all_metrics m
    INNER JOIN public.tb_work_log w 
    -- 매칭 조건
    ON m."timestamp" >= COALESCE(w.corrected_start_time, w.start_time - INTERVAL '9 hours')
    AND m."timestamp" <= (w.end_time - INTERVAL '9 hours')
    AND w.machine_id::text = '2호기(창녕)'::text
    AND (
        m.device_id = ANY (ARRAY['extruder_plc'::text, 'spot_temperature_sensor'::text])
    );

-- 4. [핵심] 소유권 및 권한 '강제' 설정 (순서 중요!)

-- 4-1. 뷰의 주인을 'postgres'로 변경 (Grafana가 보통 이 계정으로 접속함)
ALTER VIEW public.view_ml_learning_data OWNER TO postgres;

-- 4-2. 스키마 사용 권한 확인 (이게 없으면 테이블 권한 있어도 진입 불가)
GRANT USAGE ON SCHEMA public TO postgres, anon, authenticated, service_role;

-- 4-3. 뷰 조회 권한을 '모든 역할(PUBLIC)'에게 개방 (권한 에러 원천 차단)
GRANT SELECT ON public.view_ml_learning_data TO public;

-- 4-4. 뷰가 참조하는 '원본 테이블'들에 대한 권한도 모두에게 개방
GRANT SELECT ON public.all_metrics TO public;
GRANT SELECT ON public.tb_work_log TO public;

-- 5. 완료
COMMIT;