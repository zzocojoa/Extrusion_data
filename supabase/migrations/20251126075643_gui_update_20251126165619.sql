
  create table "public"."all_metrics_processed" (
    "timestamp" timestamp with time zone,
    "temperature" double precision,
    "main_pressure" double precision,
    "billet_length" double precision,
    "container_temp_front" double precision,
    "container_temp_rear" double precision,
    "production_counter" bigint,
    "current_speed" double precision,
    "extrusion_end_position" double precision
      );



