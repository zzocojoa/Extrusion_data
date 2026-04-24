-- all_metrics의 장비 범위와 Smart Sync 조회를 복구한다.

ALTER TABLE public.all_metrics
    ADD COLUMN IF NOT EXISTS device_id text;

UPDATE public.all_metrics
SET device_id = CASE
    WHEN device_id IS NOT NULL AND btrim(device_id) <> '' THEN device_id
    WHEN mold_1 IS NOT NULL
        OR mold_2 IS NOT NULL
        OR mold_3 IS NOT NULL
        OR mold_4 IS NOT NULL
        OR mold_5 IS NOT NULL
        OR mold_6 IS NOT NULL
        OR billet_temp IS NOT NULL
        OR at_pre IS NOT NULL
        OR at_temp IS NOT NULL
        OR die_id IS NOT NULL
        OR billet_cycle_id IS NOT NULL
        THEN 'extruder_integrated'
    WHEN main_pressure IS NOT NULL
        OR billet_length IS NOT NULL
        OR container_temp_front IS NOT NULL
        OR container_temp_rear IS NOT NULL
        OR production_counter IS NOT NULL
        OR current_speed IS NOT NULL
        OR extrusion_end_position IS NOT NULL
        THEN 'extruder_plc'
    WHEN temperature IS NOT NULL
        THEN 'spot_temperature_sensor'
    ELSE NULL
END
WHERE device_id IS NULL
   OR btrim(device_id) = '';

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM public.all_metrics
        WHERE device_id IS NULL
           OR btrim(device_id) = ''
    ) THEN
        RAISE EXCEPTION 'all_metrics.device_id를 결정하지 못한 행이 남아 있습니다.';
    END IF;
END $$;

ALTER TABLE public.all_metrics
    ALTER COLUMN device_id SET NOT NULL;

ALTER TABLE public.all_metrics
    DROP CONSTRAINT IF EXISTS all_metrics_pkey;

ALTER TABLE public.all_metrics
    DROP CONSTRAINT IF EXISTS unique_metrics_constraint;

ALTER TABLE public.all_metrics
    DROP CONSTRAINT IF EXISTS all_metrics_timestamp_device_id_key;

ALTER TABLE public.all_metrics
    ADD CONSTRAINT all_metrics_timestamp_device_id_key
        UNIQUE ("timestamp", device_id);

CREATE INDEX IF NOT EXISTS idx_all_metrics_latest_timestamp_by_device
    ON public.all_metrics (device_id, "timestamp" DESC);

CREATE INDEX IF NOT EXISTS idx_all_metrics_timestamp
    ON public.all_metrics ("timestamp");
