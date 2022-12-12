\if :{?jsonlog_split_sql}
\else
\set jsonlog_split_sql true

-- partitions to child-tables based on by_
-- then sets archives with stride_
--

\ir archive.sql

create procedure jsonlog.split (
    cls_ regclass,
    by_ text default 'logger_id',
    archive_by_ text default 'tz',
    archive_stride_ text default '1 year'
)
    language plpgsql
    security definer
    set search_path="$user",public
as $$
begin
    raise warning 'jsonlog.split for % by %', cls_, by_;

    -- create trigger function
    execute format('
    create or replace function %s_jsonlog_split_f() -- 1
        returns trigger
        language plpgsql
        security definer
        set search_path="$user",public
    as $fn$
    ', cls_) || format('
    declare
        t text = ''%s_'' || new.%s; -- 1, 2
    begin
        -- raise warning ''---split %%'', t;
    ', cls_, by_)|| format('

        if to_regclass(t) is null
        then
            -- raise warning ''---creating %%'', t;

            execute format(''
                create table %%s (
                    like %s including all -- 1
                ) inherits (%s) -- 2
            '', t);

            call jsonlog.archive(
                t,
                by_:=%L, -- 3
                stride_:=%L -- 4
            );
        end if;

    ', cls_, cls_, archive_by_, archive_stride_) || format('

        execute format(''
            insert into %%s
            select (%s %%s).*  -- 1
            on conflict do nothing
        '', t, quote_literal(new));

        return null;
    exception
        when others then
        raise warning ''%s_jsonlog_split_f.error %% (%%)'',sqlerrm, sqlstate; -- 2
        return null;
    end;
    $fn$;
    ', cls_, cls_);

    -- recreate trigger
    execute format('
    drop trigger if exists jsonlog_split_f_%s on %s cascade
    ',
        replace(cls_::text, '.', '_'),
        cls_
    );

    execute format('
        create trigger jsonlog_split_f_%s
        before insert on %s
        for each row execute function %s_jsonlog_split_f()
    ',
        replace(cls_::text, '.', '_'),
        cls_,
        cls_
    );
end;
$$;

-- notes:
-- since record is immediately moved to child-table, "insert ... returning"  returns a null.
-- to use archive.sql is preferred, parent table can be "most-recent" records

\endif
