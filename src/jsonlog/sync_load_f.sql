\if :{?jsonlog_create_load_f_sql}
\else
\set jsonlog_create_load_f_sql true

-- creates a single/unified load_f for all types
--
create procedure jsonlog.sync_load_f ()
    language plpgsql
    security definer
    set search_path="$user",public
as $$
declare
    t text;
    r record;
begin
    t = '
    create or replace function jsonlog.load_f(
        type_ text,
        data_ jsonb,
        param_ jsonb default null
    )
        returns int
        language sql
        security definer
        set search_path="$user",public
    as $fn$
        select case
    ';

    for r in (
        select *
        from _jsonlog.type
    ) loop
        t = t || format(
            'when type_=%L then %s ',
            r.id,
            format (
                '%s(jsonb_populate_record(null::%s, data_) %s)',
                to_regprocedure(r.load_f)::regproc,
                r.value_t,
                case
                when r.param_t is not null
                then format(', jsonb_populate_record(null::%s, param_)',
                    r.param_t)
                else ''
                end
            )
        );
    end loop;

    t = t || '
        else 0
        end
    $fn$';

    execute t;
end;
$$;


create function jsonlog.auto_sync_load_f()
    returns trigger
    language plpgsql
    security definer
as $$
begin
    call jsonlog.sync_load_f();
    return null;
end;
$$;

drop trigger if exists jsonlog_auto_sync_load_f
    on _jsonlog.type cascade;

create trigger jsonlog_auto_sync_load_f
    after insert or delete or update
    on _jsonlog.type
    for each statement
        execute function jsonlog.auto_sync_load_f();


\endif
