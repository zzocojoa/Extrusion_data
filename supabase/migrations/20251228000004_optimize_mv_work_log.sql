-- Migration: Optimize work log die_id mapping (range index + MV rebuild)
-- Date: 2025-12-28

-- Range index for faster timestamp containment lookups
CREATE INDEX IF NOT EXISTS idx_work_log_time_range
    ON public.tb_work_log
    USING GIST (
        tstzrange(
            start_time,
            COALESCE(end_time, 'infinity'::timestamptz),
            '[]'
        )
    );

DROP MATERIALIZED VIEW IF EXISTS public.mv_optimized_metrics_work_log CASCADE;

CREATE MATERIALIZED VIEW public.mv_optimized_metrics_work_log AS
SELECT
    COALESCE(w.die_id, m.die_id) AS die_id,
    m.session_id,
    m.calc_cycle_id,
    m."timestamp",
    m.main_pressure,
    m.current_speed,
    m.billet_length,
    m.container_temp_front,
    m.container_temp_rear,
    m.production_counter,
    m.extrusion_end_position,
    m.mold_1,
    m.mold_2,
    m.mold_3,
    m.mold_4,
    m.mold_5,
    m.mold_6,
    m.billet_temp,
    m.at_pre,
    m.at_temp,
    m.billet_cycle_id,
    m.original_temperature,
    m.temperature,
    m._applied_offset
FROM public.view_optimized_aligned_metrics m
LEFT JOIN LATERAL (
    SELECT wl.die_id
    FROM public.tb_work_log wl
    WHERE tstzrange(
        wl.start_time,
        COALESCE(wl.end_time, 'infinity'::timestamptz),
        '[]'
    ) @> m."timestamp"
    ORDER BY wl.created_at DESC,
             EXTRACT(EPOCH FROM (COALESCE(wl.end_time, now()) - wl.start_time)) ASC,
             wl.id DESC
    LIMIT 1
) w ON true
WITH NO DATA;

CREATE INDEX IF NOT EXISTS idx_mv_optimized_metrics_work_log_ts
    ON public.mv_optimized_metrics_work_log ("timestamp");

GRANT SELECT ON public.mv_optimized_metrics_work_log TO anon, authenticated, service_role;
