\if :{?jsonlog_sql}
\else
\set jsonlog_sql true

-- logs json into table
-- loads payload according to type
-- has auto-archive (partition by timestamp)
-- has auto-split (partition by field and then timestamp)

\ir util/date_bin.sql

\if :test
\if :local
drop schema if exists _jsonlog cascade;
\endif
\endif
create schema if not exists _jsonlog;

drop schema if exists jsonlog cascade;
create schema jsonlog;

-- contains references to table and loaders
--
create table if not exists _jsonlog.type (
    id text
        primary key,
    class_t text,
    value_t text,
    param_t text,
    load_f text
);

\ir jsonlog/type.sql
\ir jsonlog/sync_load_f.sql

create table if not exists _jsonlog.logger (
    id text
        default md5(gen_random_uuid()::text)
        primary key,
    types text[]
        -- below is yet supported!
        -- foreign key (each element of types) references _jsonlog.type,
        not null,
    param jsonb
        -- optional parameter for load_f
        -- ex: error conditions
);

-- contains raw log
--
create table if not exists _jsonlog.log (
    id text primary key
        default md5(gen_random_uuid()::text),
    tz timestamp with time zone
        default current_timestamp,
    load_tz timestamp with time zone,
    data jsonb not null
);

-- create auto-archive every 1 day
--
\ir jsonlog/archive.sql
call jsonlog.archive(
    '_jsonlog.log',
    stride_ := '1 day',
    by_ := 'tz'
);


-- loads log to corresponding type's storage
--
set app.logger_id_jsonpath = '$.logger_id';

create function jsonlog.load (
    rs_ _jsonlog.log[],
    path_ jsonpath
        default coalesce(
            current_setting('app.logger_id_jsonpath', 't'),
            '$.logger_id' -- change as needed
        )::jsonpath
)
    returns int
    language plpgsql
    security definer
    stable
as $$
declare
    n int;
begin
    -- process the log to corresponding tables
    with
    args as (
        select
            unnest(l.types) as type,
            l.param,
            jsonb_build_object(
                'id', r.id,
                'tz', r.tz
            ) || r.data as data
        from unnest(rs_) r
        join _jsonlog.logger l
            on l.id = jsonb_path_query_first(r.data, path_)->>0
    )
    select count(jsonlog.load_f(
        a.type,
        a.data,
        a.param
    ))
    into n
    from args a;

    return n;
end;
$$;

\ir jsonlog/auto_load.sql


-- creates auto-split on a field
-- then auto-archives every 1 year
--
\ir jsonlog/split.sql


\if :test
    -- a study1 measurement
    -- without custom loader, _jsonlog.data will be directly mapped
    --
    create table if not exists tests.jsonlog_study1 (
        id text,
        tz timestamp with time zone,
        logger_id text,
        logger_tz timestamp with time zone,
        cod float
    );

    -- study2 measurements
    -- with a custom loader for study2
    -- logger to have parameter
    --
    create table if not exists tests.jsonlog_study2 (
        id text,
        tz timestamp with time zone,
        logger_id text,
        nh3n float,
        errors text[]
    );
    create type tests.jsonlog_study2_param_t as (
        min_nh3n float,
        max_nh3n float
    );
    create function tests.jsonlog_study2 (
        it tests.jsonlog_study2,
        p tests.jsonlog_study2_param_t
    )
        returns int
        language plpgsql
        security definer
    as $$
    begin
        it.errors = array['test'];
        insert into tests.jsonlog_study2
            select (it).*
        on conflict do nothing;
        return 1;
    end;
    $$;

    create function tests.test_jsonlog()
        returns setof text
        language plpgsql
    as $$
    begin
        -- split and register study1
        call jsonlog.split('tests.jsonlog_study1', by_:='logger_id');
        perform jsonlog.type('tests.jsonlog_study1');

        -- split and register study2
        call jsonlog.split('tests.jsonlog_study2', by_:='logger_id');
        perform jsonlog.type(
            'tests.jsonlog_study2',
            load_f_ := 'tests.jsonlog_study2(tests.jsonlog_study2, tests.jsonlog_study2_param_t)'
        );

        -- create loggers
        insert into _jsonlog.logger (id, types, param)
        values
            ('logger1', array['tests.jsonlog_study1'], null),
            ('logger2', array['tests.jsonlog_study1', 'tests.jsonlog_study2'], jsonb_build_object(
                'min_nh3n', 100,
                'max_nh3n', 200
            ));

        -- insert some log
        insert into _jsonlog.log (data)
        values
            ('{"logger_id":"logger1","cod":111}'),
            ('{"logger_id":"logger1","cod":222}'),
            ('{"logger_id":"logger2","nh3n":333,"cod":333}'), -- this will be in study1 and study2
            ('{"logger_id":"logger2","nh3n":444,"cod":444}'),
            ('{"text":"unknown"}') -- will be logged but ignored
        ;

        return next has_table(
            'tests' , 'jsonlog_study1_logger1',
            'has study1_logger1 split'
        );
        return next has_table(
            'tests' , 'jsonlog_study1_logger2',
            'has study1_logger2 split'
        );
        return next has_table(
            'tests' , 'jsonlog_study2_logger2',
            'has study2_logger2 split'
        );

        return next ok(
            (select count(1) = 5 from _jsonlog.log),
            '5 json logs');
        return next ok(
            (select count(1) = 4 from tests.jsonlog_study1),
            '4 in study1');
        return next ok(
            (select count(1) = 2 from tests.jsonlog_study2),
            '2 in study2');

        -- when data is < stride_
        declare
            r _jsonlog.log;
            t text;
        begin
            update _jsonlog.log a
            set tz = tz - '2 days'::interval
            where data->>'text' = 'unknown'
            returning *
            into r;

            t = 'log_' || to_char(
                util.date_bin('1 day', r.tz, timestamp '2000-01-01'),
                'YYYYMMDDHH24MISS');

            return next has_table(
                '_jsonlog', t,
                'has log archive'
            );

            return next ok(
                (select count(1) = 4 from only _jsonlog.log),
                'main log table has current records'
            );
        end;
    end;
    $$;
\endif

\endif
