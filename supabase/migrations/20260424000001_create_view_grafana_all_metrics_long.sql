-- Grafana에서 재사용하기 쉬운 long format 조회 뷰를 만든다.

CREATE OR REPLACE VIEW public.view_grafana_all_metrics_long AS
SELECT
    m."timestamp",
    m.device_id,
    m.die_id,
    m.billet_cycle_id,
    metric.metric_name,
    metric.metric_value
FROM public.all_metrics AS m
CROSS JOIN LATERAL (
    VALUES
        ('temperature', m.temperature),
        ('main_pressure', m.main_pressure),
        ('billet_length', m.billet_length),
        ('container_temp_front', m.container_temp_front),
        ('container_temp_rear', m.container_temp_rear),
        ('production_counter', m.production_counter::double precision),
        ('current_speed', m.current_speed),
        ('extrusion_end_position', m.extrusion_end_position),
        ('mold_1', m.mold_1),
        ('mold_2', m.mold_2),
        ('mold_3', m.mold_3),
        ('mold_4', m.mold_4),
        ('mold_5', m.mold_5),
        ('mold_6', m.mold_6),
        ('billet_temp', m.billet_temp),
        ('at_pre', m.at_pre),
        ('at_temp', m.at_temp)
) AS metric(metric_name, metric_value)
WHERE metric.metric_value IS NOT NULL;

COMMENT ON VIEW public.view_grafana_all_metrics_long IS 'Grafana 패널에서 바로 쓰기 위한 all_metrics long format 뷰';
COMMENT ON COLUMN public.view_grafana_all_metrics_long."timestamp" IS '원본 측정 시각';
COMMENT ON COLUMN public.view_grafana_all_metrics_long.device_id IS '측정 원본 장치 식별자';
COMMENT ON COLUMN public.view_grafana_all_metrics_long.die_id IS '금형 식별자';
COMMENT ON COLUMN public.view_grafana_all_metrics_long.billet_cycle_id IS '빌렛 사이클 식별자';
COMMENT ON COLUMN public.view_grafana_all_metrics_long.metric_name IS 'Grafana 시리즈 구분용 메트릭 이름';
COMMENT ON COLUMN public.view_grafana_all_metrics_long.metric_value IS 'Grafana 시계열 값';

GRANT SELECT ON public.view_grafana_all_metrics_long TO anon, authenticated, service_role;
