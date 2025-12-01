alter table "public"."all_metrics" drop constraint "all_metrics_timestamp_device_id_key";

drop view if exists "public"."view_ml_learning_data";

drop index if exists "public"."all_metrics_timestamp_device_id_key";


  create table "public"."temp_machine_starts" (
    "plc_start_time" timestamp with time zone
      );


alter table "public"."all_metrics" drop column "new_sensor_value";

CREATE INDEX idx_temp_starts ON public.temp_machine_starts USING btree (plc_start_time);

create or replace view "public"."view_ml_learning_data" as  SELECT m."timestamp",
    m.device_id,
    m.main_pressure,
    m.current_speed,
    m.billet_length,
    m.temperature AS actual_exit_temp,
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
        CASE
            WHEN (w.production_weight > (100000)::numeric) THEN (w.production_weight / 1000.0)
            ELSE w.production_weight
        END AS production_weight,
        CASE
            WHEN (w.productivity > (100000)::numeric) THEN (w.productivity / 1000.0)
            ELSE w.productivity
        END AS productivity,
    COALESCE(w.corrected_start_time, (w.start_time - '09:00:00'::interval)) AS start_time,
    (w.end_time - '09:00:00'::interval) AS end_time
   FROM (public.all_metrics m
     JOIN public.tb_work_log w ON (((m."timestamp" >= COALESCE(w.corrected_start_time, (w.start_time - '09:00:00'::interval))) AND (m."timestamp" <= (w.end_time - '09:00:00'::interval)) AND ((w.machine_id)::text = '2호기(창녕)'::text) AND (m.device_id = ANY (ARRAY['extruder_plc'::text, 'spot_temperature_sensor'::text])))));


grant select on table "public"."all_metrics" to "grafana";

grant delete on table "public"."temp_machine_starts" to "anon";

grant insert on table "public"."temp_machine_starts" to "anon";

grant references on table "public"."temp_machine_starts" to "anon";

grant select on table "public"."temp_machine_starts" to "anon";

grant trigger on table "public"."temp_machine_starts" to "anon";

grant truncate on table "public"."temp_machine_starts" to "anon";

grant update on table "public"."temp_machine_starts" to "anon";

grant delete on table "public"."temp_machine_starts" to "authenticated";

grant insert on table "public"."temp_machine_starts" to "authenticated";

grant references on table "public"."temp_machine_starts" to "authenticated";

grant select on table "public"."temp_machine_starts" to "authenticated";

grant trigger on table "public"."temp_machine_starts" to "authenticated";

grant truncate on table "public"."temp_machine_starts" to "authenticated";

grant update on table "public"."temp_machine_starts" to "authenticated";

grant delete on table "public"."temp_machine_starts" to "service_role";

grant insert on table "public"."temp_machine_starts" to "service_role";

grant references on table "public"."temp_machine_starts" to "service_role";

grant select on table "public"."temp_machine_starts" to "service_role";

grant trigger on table "public"."temp_machine_starts" to "service_role";

grant truncate on table "public"."temp_machine_starts" to "service_role";

grant update on table "public"."temp_machine_starts" to "service_role";


