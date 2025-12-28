-- Drop dependent views
DROP VIEW IF EXISTS "public"."view_ml_learning_data";

-- Modify all_metrics table
ALTER TABLE "public"."all_metrics" DROP CONSTRAINT IF EXISTS "unique_metrics_constraint";
ALTER TABLE "public"."all_metrics" DROP COLUMN IF EXISTS "device_id";

-- Set Timestamp as Primary Key
-- Set Timestamp as Primary Key (Safely)
DO $$
BEGIN
    BEGIN
        ALTER TABLE "public"."all_metrics" ADD CONSTRAINT "all_metrics_pkey" PRIMARY KEY ("timestamp");
    EXCEPTION WHEN OTHERS THEN
        NULL; -- Ignore if PK already exists
    END;
END $$;
