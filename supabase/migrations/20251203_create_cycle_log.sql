-- Create tb_cycle_log table to store segmented cycle metadata
CREATE TABLE IF NOT EXISTS "public"."tb_cycle_log" (
    "id" uuid DEFAULT uuid_generate_v4() NOT NULL PRIMARY KEY,
    "created_at" timestamp with time zone DEFAULT now(),
    "machine_id" text NOT NULL,
    "start_time" timestamp with time zone NOT NULL,
    "end_time" timestamp with time zone NOT NULL,
    "production_counter" bigint,
    "work_log_id" bigint REFERENCES "public"."tb_work_log"("id"),
    "duration_sec" double precision,
    "max_pressure" double precision,
    "is_valid" boolean DEFAULT false,
    "is_test_run" boolean DEFAULT false,
    "segmentation_method" text DEFAULT 'pressure_threshold_30bar'
);

-- Add indexes for performance
CREATE INDEX IF NOT EXISTS idx_cycle_log_machine_time ON "public"."tb_cycle_log" ("machine_id", "start_time");
CREATE INDEX IF NOT EXISTS idx_cycle_log_work_log_id ON "public"."tb_cycle_log" ("work_log_id");

-- Enable RLS
ALTER TABLE "public"."tb_cycle_log" ENABLE ROW LEVEL SECURITY;

-- Policy: Allow read access to authenticated users
CREATE POLICY "Enable read access for all users" ON "public"."tb_cycle_log"
    FOR SELECT USING (true);

-- Policy: Allow insert/update for authenticated users (or service role)
CREATE POLICY "Enable insert for authenticated users only" ON "public"."tb_cycle_log"
    FOR INSERT WITH CHECK (auth.role() = 'authenticated' OR auth.role() = 'service_role');

CREATE POLICY "Enable update for authenticated users only" ON "public"."tb_cycle_log"
    FOR UPDATE USING (auth.role() = 'authenticated' OR auth.role() = 'service_role');
