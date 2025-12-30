-- Migration: Create mv_work_log_effective_ranges for fast exact-range matching
-- Date: 2025-12-30

DROP MATERIALIZED VIEW IF EXISTS public.mv_work_log_effective_ranges CASCADE;

CREATE MATERIALIZED VIEW public.mv_work_log_effective_ranges AS
WITH work_log AS (
    SELECT
        id,
        die_id,
        start_time,
        COALESCE(end_time, now()) AS end_time,
        created_at,
        EXTRACT(EPOCH FROM (COALESCE(end_time, now()) - start_time)) AS duration_sec
    FROM public.tb_work_log
    WHERE start_time IS NOT NULL
),
bounds AS (
    SELECT start_time AS ts FROM work_log
    UNION
    SELECT end_time AS ts FROM work_log
),
ordered_bounds AS (
    SELECT
        ts,
        LEAD(ts) OVER (ORDER BY ts) AS next_ts
    FROM bounds
),
segments AS (
    SELECT
        ts AS seg_start,
        next_ts AS seg_end
    FROM ordered_bounds
    WHERE next_ts IS NOT NULL
      AND next_ts > ts
),
resolved AS (
    SELECT
        s.seg_start,
        s.seg_end,
        wl.die_id,
        wl.id AS work_log_id
    FROM segments s
    JOIN LATERAL (
        SELECT wl.*
        FROM work_log wl
        WHERE wl.start_time <= s.seg_start
          AND wl.end_time >= s.seg_end
        ORDER BY wl.created_at DESC, wl.duration_sec ASC, wl.id DESC
        LIMIT 1
    ) wl ON true
)
SELECT
    seg_start AS range_start,
    seg_end AS range_end,
    die_id,
    work_log_id,
    tstzrange(seg_start, seg_end, '[]') AS period
FROM resolved
WITH NO DATA;

CREATE INDEX IF NOT EXISTS idx_mv_work_log_effective_ranges_period
    ON public.mv_work_log_effective_ranges
    USING GIST (period);

CREATE INDEX IF NOT EXISTS idx_mv_work_log_effective_ranges_start
    ON public.mv_work_log_effective_ranges (range_start);

GRANT SELECT ON public.mv_work_log_effective_ranges TO anon, authenticated, service_role;
