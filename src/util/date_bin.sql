
\if :{?util_date_bin_sql}
\else
\set util_date_bin_sql true

create schema if not exists util;

-- pg's date_bin throws when stride contains month or year
--

create or replace function util.date_bin (
    stride interval,
    tz timestamp with time zone,
    base_tz  timestamp with time zone
)
    returns timestamp with time zone
    language sql
    immutable
as $$
select
    base_tz
    + floor(extract(epoch from tz - base_tz) / extract(epoch from stride))::bigint
    * stride;
$$;

\endif