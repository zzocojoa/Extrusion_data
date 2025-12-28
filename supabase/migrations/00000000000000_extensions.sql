-- Required extensions for Supabase local development.
create schema if not exists "extensions";

create extension if not exists "pgcrypto" with schema "extensions";
create extension if not exists "pgjwt" with schema "extensions";
create extension if not exists "uuid-ossp" with schema "extensions";
