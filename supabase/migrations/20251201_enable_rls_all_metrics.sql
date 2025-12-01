-- Enable RLS
ALTER TABLE public.all_metrics ENABLE ROW LEVEL SECURITY;

-- Policy for INSERT (Allow anon key to insert)
CREATE POLICY "Allow anon insert" ON public.all_metrics
FOR INSERT
TO anon
WITH CHECK (true);

-- Policy for SELECT (Allow anon key to select for Smart Sync)
CREATE POLICY "Allow anon select" ON public.all_metrics
FOR SELECT
TO anon
USING (true);
