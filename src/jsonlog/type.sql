\if :{?jsonlog_type_sql}
\else
\set jsonlog_type_sql true

create function jsonlog.type (
    id_ text,
    class_t_ regclass default null,
    value_t_ regtype default null,
    param_t_ regtype default null,
    load_f_ regprocedure default null
)
    returns _jsonlog.type
    language plpgsql
    security definer
    set search_path="$user",public
as $$
declare
    r _jsonlog.type;
    t text;
begin
    r.id = id_;
    r.class_t = class_t_;
    r.value_t = value_t_;
    r.param_t = param_t_;
    r.load_f = load_f_;

    call jsonlog.sync(r);

    insert into _jsonlog.type
        select (r).*
    on conflict (id)
    do update set
        class_t = r.class_t::text,
        value_t = r.value_t::text,
        param_t = r.param_t::text,
        load_f = r.load_f::text
    returning *
    into r;

    return r;
end;
$$;

create procedure jsonlog.sync(
    in out r _jsonlog.type
)
    language plpgsql
    security definer
as $$
declare
    t text;
    ts text[];
begin
    -- extract value_t/param_t
    --
    if not (to_regprocedure(r.load_f) is null)
    and (r.value_t is null or r.param_t is null)
    then
        select array_agg(args)
        into ts
        from (
            select format_type(unnest(proargtypes), null) args
            from pg_proc
            where oid = r.load_f::regprocedure::oid
        ) x;

        if not (to_regtype(ts[1]) is null)
        and r.value_t is null
        then
            r.value_t = ts[1];
        end if;

        if not (to_regtype(ts[2]) is null)
        and r.param_t is null
        then
            r.param_t = ts[2];
        end if;
    end if;

    -- check values
    --
    r.class_t = coalesce(r.class_t, to_regclass(r.id)::text);
    r.value_t = coalesce(r.value_t, r.class_t);
    r.load_f = coalesce(
        r.load_f,
        to_regprocedure(
            case
            when r.param_t is not null
            then format('%s(%s,%s)', r.class_t, r.value_t, r.param_t)
            else format('%s(%s)', r.class_t, r.value_t)
            end
        )::text
    );

    -- if load_f not found, default to load directly to table
    --
    if r.load_f is null
    then
        t = format('jsonlog.load_f(%s %s)',
            r.value_t,
            case
            when r.param_t is null then ''
            else format(', %s', r.param_t)
            end
        );

        execute format('
        create or replace function %s -- 1
            returns int
            language sql
            security definer
        as $fn$
            with
            inserted as (
                insert into %s select ($1).* -- 2
                on conflict do nothing
            )
            select 1
        $fn$
        ', t, r.class_t);

        r.load_f = to_regprocedure(t);
    end if;
end;
$$;


\endif
