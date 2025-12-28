-- Migration: Fix tb_work_log column types based on XLSM data review
-- Date: 2025-12-28
-- Description: Align numeric types with observed values in 압출일보 2호기(251223).xlsm.

ALTER TABLE public.tb_work_log
    ALTER COLUMN production_weight TYPE double precision USING production_weight::double precision,
    ALTER COLUMN productivity TYPE double precision USING productivity::double precision;

ALTER TABLE public.tb_work_log
    ALTER COLUMN production_qty TYPE integer USING production_qty::integer,
    ALTER COLUMN die_number TYPE integer USING die_number::integer,
    ALTER COLUMN product_length TYPE integer USING product_length::integer,
    ALTER COLUMN start_cut TYPE integer USING start_cut::integer,
    ALTER COLUMN end_cut TYPE integer USING end_cut::integer;

ALTER TABLE public.tb_work_log
    ALTER COLUMN defect_bubble TYPE integer USING defect_bubble::integer,
    ALTER COLUMN defect_tearing TYPE integer USING defect_tearing::integer,
    ALTER COLUMN defect_white_black_line TYPE integer USING defect_white_black_line::integer,
    ALTER COLUMN defect_oxide TYPE integer USING defect_oxide::integer,
    ALTER COLUMN defect_scratch TYPE integer USING defect_scratch::integer,
    ALTER COLUMN defect_bend TYPE integer USING defect_bend::integer,
    ALTER COLUMN defect_dimension TYPE integer USING defect_dimension::integer,
    ALTER COLUMN defect_line TYPE integer USING defect_line::integer,
    ALTER COLUMN defect_etc TYPE integer USING defect_etc::integer;
