\if :{?jsonlog_archive_sql}
\else
\set jsonlog_archive_sql true

-- to do an auto-archive to child-tables
-- with by_ a timestamp with time zone,
-- binned with stride_
--

create procedure jsonlog.archive(
    cls_ regclass,
    stride_ text default '1 day',
    by_ text default 'tz'
)
    language plpgsql
    security definer
    set search_path="$user", public
as $$
begin
    if by_ is null
    or stride_ is null
    then
        return;
    end if;


    raise warning 'jsonlog.archive for % on % over %', cls_, by_, stride_;

    execute format('
    create or replace function %s_jsonlog_archive_f() -- 1
        returns trigger
        language plpgsql
        security definer
        set search_path="$user",public
    as $fn$
    ', cls_) || format('
    declare
        min_tz timestamp with time zone = util.date_bin(
            %L::interval,  -- 1
            current_timestamp,
            timestamp ''2000-01-01''
        );
        r record;
        t text;
    begin
    ', stride_) || format('

        for r in (
            with
            deleted as (
                delete from only %s -- 1
                where %s < min_tz -- 2
                returning *
            )
            select *
            from deleted
        ) loop

    ', cls_, by_) || format('

            t = ''%s_'' || to_char(util.date_bin(  -- 1
                %L, -- 2
                r.%s, -- 3
                timestamp ''2000-01-01''
            ), ''YYYYMMDDHH24MISS'');

            -- raise warning ''---archiving %%'', t;

    ', cls_, stride_, by_) || format('

            if to_regclass(t) is null
            then
                execute format(''
                    create table %%s (
                        like %s including all -- 1
                    ) inherits (%s) -- 2
                '', t);
            end if;

    ', cls_, cls_) || format('

            execute format(''
                insert into %%s
                select (%s %%s).* -- 1
                on conflict do nothing
            '', t, quote_literal(r));

    ', cls_) || format('
        end loop;
        return null;
    exception
        when others then
        raise warning ''%s_jsonlog_archive_f %% %%'',sqlerrm, sqlstate;
        return null;
    end;
    $fn$;
    ', cls_);

    execute format('
    drop trigger if exists jsonlog_archive_f_%s on %s cascade
    ',
        replace(cls_::text, '.', '_'),
        cls_
    );

    execute format('
    create trigger jsonlog_archive_f_%s
    after insert or update on %s
    for each statement
        execute function %s_jsonlog_archive_f()
    ',
        replace(cls_::text, '.', '_'),
        cls_,
        cls_
    );

end;
$$;

\endif
