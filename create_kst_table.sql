-- [Create KST Table with Sort-Enforcing PK]
-- Issue: Supabase Table Editor shows rows in random order if no PK or explicit sort exists.
-- Fix: Create 'id' based on Time Order and make it Primary Key. Supabase defaults to PK sort.

DROP TABLE IF EXISTS public.all_metrics_kst;

CREATE TABLE public.all_metrics_kst AS
SELECT
  -- 1. Create ID based on Time Order (This forces UI to sort by Time if it sorts by ID)
  ROW_NUMBER() OVER (ORDER BY "timestamp" ASC) as id,
  
  -- 2. Converted Time Column (UTC -> KST Wall Clock)
  -- 'timestamp' is timestamptz (UTC). 
  -- AT TIME ZONE 'Asia/Seoul' converts it to 'timestamp without time zone' showing local wall clock time.
  "timestamp" AT TIME ZONE 'Asia/Seoul' as timestamp_kst,
  
  -- 3. All other columns
  *
FROM
  public.all_metrics
ORDER BY
  "timestamp" ASC;

-- [Set Primary Key]
-- This is critical for Supabase Table Editor to behave correctly
ALTER TABLE public.all_metrics_kst ADD PRIMARY KEY (id);

-- [Permissions]
GRANT SELECT ON public.all_metrics_kst TO anon, authenticated, service_role;
