-- The ingestr benchmark table, verbatim: schema from ingestr's benchmarks/sql/postgres_seed.sql,
-- value generators ported 1:1 from their benchmarks/sql/duckdb_seed.sql (same distributions, same
-- string shapes) so the comparison runs on identical data. Placeholders: BENCH_TABLE_PLACEHOLDER,
-- BENCH_ROWS_PLACEHOLDER.

DROP TABLE IF EXISTS public.BENCH_TABLE_PLACEHOLDER;

CREATE TABLE public.BENCH_TABLE_PLACEHOLDER (
    id              INTEGER PRIMARY KEY,
    small_str       VARCHAR(20),
    medium_str      VARCHAR(100),
    large_str       VARCHAR(500),
    tiny_int        SMALLINT,
    regular_int     INTEGER,
    big_int         BIGINT,
    float_val       DOUBLE PRECISION,
    decimal_val     NUMERIC(18,4),
    bool_val        BOOLEAN,
    date_val        DATE,
    ts_val          TIMESTAMP,
    ts_tz_val       TIMESTAMPTZ,
    json_val        JSONB,
    extra_text      TEXT
);

INSERT INTO public.BENCH_TABLE_PLACEHOLDER
SELECT
    g::integer                                                            AS id,
    'name_' || (g % 10000)                                                AS small_str,
    'user_' || g || '@example-' || (g % 500) || '.com'                    AS medium_str,
    repeat(chr(65 + (g % 26)::integer), (50 + (g % 200))::integer)        AS large_str,
    (g % 32767)::smallint                                                 AS tiny_int,
    g::integer                                                            AS regular_int,
    (g::bigint * 1000000)                                                 AS big_int,
    (g::float8 / 7.0) + (g % 1000)::float8                                AS float_val,
    ((g % 1000000)::numeric(18,4)) / 100.0                                AS decimal_val,
    (g % 2 = 0)                                                           AS bool_val,
    (date '2020-01-01' + (g % 1500)::integer)                             AS date_val,
    (timestamp '2020-01-01' + make_interval(secs => g))                   AS ts_val,
    (timestamptz '2020-01-01 00:00:00+00' + make_interval(secs => g))     AS ts_tz_val,
    ('{"key": "val_' || (g % 100) || '", "num": ' || g || '}')::jsonb     AS json_val,
    'extra_text_row_' || g || '_' || repeat('x', (50 + (g % 100))::integer) AS extra_text
FROM generate_series(1, BENCH_ROWS_PLACEHOLDER) AS t(g);

ANALYZE public.BENCH_TABLE_PLACEHOLDER;
