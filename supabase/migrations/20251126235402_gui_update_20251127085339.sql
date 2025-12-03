revoke delete on table "public"."all_metrics_processed" from "anon";

revoke insert on table "public"."all_metrics_processed" from "anon";

revoke references on table "public"."all_metrics_processed" from "anon";

revoke select on table "public"."all_metrics_processed" from "anon";

revoke trigger on table "public"."all_metrics_processed" from "anon";

revoke truncate on table "public"."all_metrics_processed" from "anon";

revoke update on table "public"."all_metrics_processed" from "anon";

revoke delete on table "public"."all_metrics_processed" from "authenticated";

revoke insert on table "public"."all_metrics_processed" from "authenticated";

revoke references on table "public"."all_metrics_processed" from "authenticated";

revoke select on table "public"."all_metrics_processed" from "authenticated";

revoke trigger on table "public"."all_metrics_processed" from "authenticated";

revoke truncate on table "public"."all_metrics_processed" from "authenticated";

revoke update on table "public"."all_metrics_processed" from "authenticated";

-- revoke select on table "public"."all_metrics_processed" from "grafana";

revoke delete on table "public"."all_metrics_processed" from "service_role";

revoke insert on table "public"."all_metrics_processed" from "service_role";

revoke references on table "public"."all_metrics_processed" from "service_role";

revoke select on table "public"."all_metrics_processed" from "service_role";

revoke trigger on table "public"."all_metrics_processed" from "service_role";

revoke truncate on table "public"."all_metrics_processed" from "service_role";

revoke update on table "public"."all_metrics_processed" from "service_role";

drop materialized view if exists "public"."all_metrics_processed";
drop table if exists "public"."all_metrics_processed";

set check_function_bodies = off;

create materialized view "public"."all_metrics_processed" as  WITH ordered AS (
         SELECT all_metrics."timestamp",
            all_metrics.device_id,
            all_metrics.temperature,
            all_metrics.main_pressure,
            all_metrics.billet_length,
            all_metrics.container_temp_front,
            all_metrics.container_temp_rear,
            all_metrics.production_counter,
            all_metrics.current_speed,
            all_metrics.extrusion_end_position
           FROM public.all_metrics
          ORDER BY all_metrics."timestamp"
        ), pivoted AS (
         SELECT ordered."timestamp",
            max(
                CASE
                    WHEN (ordered.device_id = 'spot_temperature_sensor'::text) THEN ordered.temperature
                    ELSE NULL::double precision
                END) AS temperature,
            max(
                CASE
                    WHEN (ordered.device_id = 'extruder_plc'::text) THEN ordered.main_pressure
                    ELSE NULL::double precision
                END) AS main_pressure,
            max(
                CASE
                    WHEN (ordered.device_id = 'extruder_plc'::text) THEN ordered.billet_length
                    ELSE NULL::double precision
                END) AS billet_length,
            max(
                CASE
                    WHEN (ordered.device_id = 'extruder_plc'::text) THEN ordered.container_temp_front
                    ELSE NULL::double precision
                END) AS container_temp_front,
            max(
                CASE
                    WHEN (ordered.device_id = 'extruder_plc'::text) THEN ordered.container_temp_rear
                    ELSE NULL::double precision
                END) AS container_temp_rear,
            max(
                CASE
                    WHEN (ordered.device_id = 'extruder_plc'::text) THEN ordered.production_counter
                    ELSE NULL::bigint
                END) AS production_counter,
            max(
                CASE
                    WHEN (ordered.device_id = 'extruder_plc'::text) THEN ordered.current_speed
                    ELSE NULL::double precision
                END) AS current_speed,
            max(
                CASE
                    WHEN (ordered.device_id = 'extruder_plc'::text) THEN ordered.extrusion_end_position
                    ELSE NULL::double precision
                END) AS extrusion_end_position
           FROM ordered
          GROUP BY ordered."timestamp"
        ), ffill AS (
         SELECT pivoted."timestamp",
            COALESCE(pivoted.temperature, lag(pivoted.temperature) OVER (ORDER BY pivoted."timestamp")) AS temperature,
            COALESCE(pivoted.main_pressure, lag(pivoted.main_pressure) OVER (ORDER BY pivoted."timestamp")) AS main_pressure,
            COALESCE(pivoted.billet_length, lag(pivoted.billet_length) OVER (ORDER BY pivoted."timestamp")) AS billet_length,
            COALESCE(pivoted.container_temp_front, lag(pivoted.container_temp_front) OVER (ORDER BY pivoted."timestamp")) AS container_temp_front,
            COALESCE(pivoted.container_temp_rear, lag(pivoted.container_temp_rear) OVER (ORDER BY pivoted."timestamp")) AS container_temp_rear,
            COALESCE(pivoted.production_counter, lag(pivoted.production_counter) OVER (ORDER BY pivoted."timestamp")) AS production_counter,
            COALESCE(pivoted.current_speed, lag(pivoted.current_speed) OVER (ORDER BY pivoted."timestamp")) AS current_speed,
            COALESCE(pivoted.extrusion_end_position, lag(pivoted.extrusion_end_position) OVER (ORDER BY pivoted."timestamp")) AS extrusion_end_position
           FROM pivoted
        ), interpolated AS (
         SELECT ffill."timestamp",
                CASE
                    WHEN ((ffill.temperature IS NULL) AND (lag(ffill.temperature) OVER (ORDER BY ffill."timestamp") IS NOT NULL) AND (lead(ffill.temperature) OVER (ORDER BY ffill."timestamp") IS NOT NULL)) THEN ((lag(ffill.temperature) OVER (ORDER BY ffill."timestamp") + lead(ffill.temperature) OVER (ORDER BY ffill."timestamp")) / (2)::double precision)
                    ELSE ffill.temperature
                END AS temperature,
                CASE
                    WHEN ((ffill.main_pressure IS NULL) AND (lag(ffill.main_pressure) OVER (ORDER BY ffill."timestamp") IS NOT NULL) AND (lead(ffill.main_pressure) OVER (ORDER BY ffill."timestamp") IS NOT NULL)) THEN ((lag(ffill.main_pressure) OVER (ORDER BY ffill."timestamp") + lead(ffill.main_pressure) OVER (ORDER BY ffill."timestamp")) / (2)::double precision)
                    ELSE ffill.main_pressure
                END AS main_pressure,
            ffill.billet_length,
            ffill.container_temp_front,
            ffill.container_temp_rear,
            ffill.production_counter,
            ffill.current_speed,
            ffill.extrusion_end_position
           FROM ffill
        )
 SELECT interpolated."timestamp",
    interpolated.temperature,
    interpolated.main_pressure,
    interpolated.billet_length,
    interpolated.container_temp_front,
    interpolated.container_temp_rear,
    interpolated.production_counter,
    interpolated.current_speed,
    interpolated.extrusion_end_position
   FROM interpolated
  ORDER BY interpolated."timestamp";


CREATE OR REPLACE FUNCTION public.grant_view_select()
 RETURNS event_trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
  EXECUTE (
    SELECT string_agg(format('GRANT SELECT ON %I.%I TO grafana', schemaname, viewname), '; ')
    FROM pg_views
    WHERE schemaname = 'public'
  );
END;
$function$
;

create or replace view "public"."metrics_view" as  SELECT all_metrics_processed."timestamp",
    all_metrics_processed.temperature,
    all_metrics_processed.main_pressure,
    all_metrics_processed.billet_length,
    all_metrics_processed.container_temp_front,
    all_metrics_processed.container_temp_rear,
    all_metrics_processed.production_counter,
    all_metrics_processed.current_speed,
    all_metrics_processed.extrusion_end_position
   FROM public.all_metrics_processed;


grant select on table "public"."all_metrics" to "supabase_admin";

-- grant select on table "public"."tb_work_log" to "grafana";

grant select on table "public"."tb_work_log" to "supabase_admin";

-- grant select on table "public"."temp_machine_starts" to "grafana";

grant select on table "public"."temp_machine_starts" to "supabase_admin";


