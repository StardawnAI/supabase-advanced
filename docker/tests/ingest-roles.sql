-- Roles the Supabase stack provides and the ingest schema grants to.
-- Created here so the end-to-end test exercises the grant path instead of
-- skipping it, and so the row level security check has a role to test with
-- that is not the owner.
create role anon nologin;
create role authenticated nologin;
create role service_role nologin bypassrls;
