-- Migration: Update mv_optimized_metrics_work_log to use effective ranges + tolerance fallback
-- Date: 2025-12-30

DROP MATERIALIZED VIEW IF EXISTS public.mv_optimized_metrics_work_log CASCADE;

CREATE MATERIALIZED VIEW public.mv_optimized_metrics_work_log AS
SELECT
    COALESCE(exact.die_id, tol.die_id, m.die_id) AS die_id,
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
    SELECT r.die_id
    FROM public.mv_work_log_effective_ranges r
    WHERE r.period @> m."timestamp"
    LIMIT 1
) exact ON true
LEFT JOIN LATERAL (
    SELECT
        wl.die_id,
        CASE
            WHEN m."timestamp" >= wl.start_time
             AND m."timestamp" <= COALESCE(wl.end_time, 'infinity'::timestamptz)
            THEN 0
            ELSE 1
        END AS match_priority,
        CASE
            WHEN m."timestamp" < wl.start_time
            THEN EXTRACT(EPOCH FROM (wl.start_time - m."timestamp"))
            WHEN m."timestamp" > COALESCE(wl.end_time, 'infinity'::timestamptz)
            THEN EXTRACT(EPOCH FROM (m."timestamp" - COALESCE(wl.end_time, 'infinity'::timestamptz)))
            ELSE 0
        END AS gap_sec,
        wl.created_at,
        EXTRACT(EPOCH FROM (COALESCE(wl.end_time, now()) - wl.start_time)) AS duration_sec,
        wl.id
    FROM public.tb_work_log wl
    WHERE exact.die_id IS NULL
      AND tstzrange(
          wl.start_time - interval '35 min',
          COALESCE(wl.end_time, 'infinity'::timestamptz) + interval '35 min',
          '[]'
      ) @> m."timestamp"
    ORDER BY
        match_priority,
        gap_sec,
        wl.created_at DESC,
        duration_sec ASC,
        wl.id DESC
    LIMIT 1
) tol ON true
WITH NO DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_optimized_metrics_work_log_ts
    ON public.mv_optimized_metrics_work_log ("timestamp");

GRANT SELECT ON public.mv_optimized_metrics_work_log TO anon, authenticated, service_role;
