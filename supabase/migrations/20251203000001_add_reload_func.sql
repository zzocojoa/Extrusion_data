-- Function to reload PostgREST schema cache
CREATE OR REPLACE FUNCTION public.reload_schema()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
  NOTIFY pgrst, 'reload schema';
END;
$$;
