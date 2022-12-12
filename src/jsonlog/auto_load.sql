\if :{?jsonlog_auto_load_sql}
\else
\set jsonlog_auto_load_sql true

-- add a trigger to load log
--
create function jsonlog.auto_load_f()
    returns trigger
    language plpgsql
    security definer
as $$
declare
    rs _jsonlog.log[];
begin
    with
    processed as (
        update _jsonlog.log
        set load_tz = current_timestamp
        where load_tz is null
        returning *
    )
    select array_agg(p)
    into rs
    from processed p;

    perform jsonlog.load(rs);
    return null;
end;
$$;

drop trigger if exists jsonlog_auto_load_f
    on _jsonlog.log cascade;

create trigger jsonlog_auto_load_f
    after insert
    on _jsonlog.log
    for each statement
        execute function jsonlog.auto_load_f();

\endif
