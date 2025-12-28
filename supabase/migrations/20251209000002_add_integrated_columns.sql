ALTER TABLE "public"."all_metrics"
ADD COLUMN IF NOT EXISTS "mold_1" double precision,
ADD COLUMN IF NOT EXISTS "mold_2" double precision,
ADD COLUMN IF NOT EXISTS "mold_3" double precision,
ADD COLUMN IF NOT EXISTS "mold_4" double precision,
ADD COLUMN IF NOT EXISTS "mold_5" double precision,
ADD COLUMN IF NOT EXISTS "mold_6" double precision,
ADD COLUMN IF NOT EXISTS "billet_temp" double precision,
ADD COLUMN IF NOT EXISTS "at_pre" double precision,
ADD COLUMN IF NOT EXISTS "at_temp" double precision;
