--
-- PostgreSQL database dump
--

\restrict Zg3WdrQr5k2sjQOC9Eh81cfNhZ6NELcdEGbPqEcbhEzdxrR1w2PxUUPJt2tjVLZ

-- Dumped from database version 17.6
-- Dumped by pg_dump version 17.6

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: all_metrics; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.all_metrics (
    "timestamp" timestamp with time zone NOT NULL,
    device_id text NOT NULL,
    temperature double precision,
    main_pressure double precision,
    billet_length double precision,
    container_temp_front double precision,
    container_temp_rear double precision,
    production_counter bigint,
    current_speed double precision
);


ALTER TABLE public.all_metrics OWNER TO postgres;

--
-- Name: all_metrics all_metrics_timestamp_device_id_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.all_metrics
    ADD CONSTRAINT all_metrics_timestamp_device_id_key UNIQUE ("timestamp", device_id);


--
-- Name: TABLE all_metrics; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.all_metrics TO anon;
GRANT ALL ON TABLE public.all_metrics TO authenticated;
GRANT ALL ON TABLE public.all_metrics TO service_role;


--
-- PostgreSQL database dump complete
--

\unrestrict Zg3WdrQr5k2sjQOC9Eh81cfNhZ6NELcdEGbPqEcbhEzdxrR1w2PxUUPJt2tjVLZ

