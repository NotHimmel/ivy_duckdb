-- ============================================================================
-- IvorySQL Oracle types x pg_duckdb execution-path regression suite
--
-- Difference from ivorysql_types.sql: that one runs on stock PostgreSQL and
-- only covers the guard path where the ivory branch is skipped; this suite must
-- run on a real IvorySQL instance (needs the sys schema) and covers the actual
--
-- Oracle type mapping and round-trip restoration in pgduckdb_types.cpp.
-- Run (not in the default schedule; cannot run on stock PG):
--     PGPORT=<oracle-mode port> make -C test/regression check-regression-ivorysql
-- The target instance needs shared_preload_libraries with liboracle_parser,
--
-- ivorysql_ora, pg_duckdb. pg_regress --temp-instance cannot be used: its initdb
-- produces a native-PG-mode cluster.
--
-- Target version: IvorySQL V1 (1.22 / pg14). Section 0 prints the Oracle types
-- this instance provides, so a version switch diffs at the top instead of
-- scattering baffling failures through the rest of the run.
--
--
-- CHARACTERIZATION SUITE: expected/ records current actual behaviour, including
-- known defects (fallbacks, hard errors). The point is to pin behaviour down:
-- fixing a defect must update expected/ so the change shows up in the diff, and
-- an accidental regression becomes visible immediately.
--
-- HOW TO READ RESULTS: section 2 lists engine + native baseline per check
-- (engine=duckdb means vectorized execution really ran; engine=postgres means
-- binding failed and it fell back - possibly correct, but not accelerated).
--
-- Sections 3-7 run the same queries under force_execution=true: output matching
-- the native column is correct; a differing value is a correctness defect; an
-- ERROR is a user-visible hard failure without fallback.
-- No automated comparison: plpgsql BEGIN...EXCEPTION opens a subtransaction and
-- pg_duckdb rejects subtransactions, which would make every query fall back, so
-- ============================================================================

\set VERBOSITY terse
SET client_min_messages = error;
SET TIME ZONE 'UTC';

-- This must succeed. "cannot be changed in native PG mode" means the target
-- instance was initdb'ed in native PG mode and Oracle types do not exist.
SET ivorysql.compatible_mode = oracle;

-- The probe functions run EXPLAIN inside plpgsql to detect the engine; without
-- this GUC pg_duckdb never takes over inside functions and every check would
SET duckdb.unsafe_allow_execution_inside_functions = true;
SET duckdb.force_execution = false;

-- ---------------------------------------------------------------------------
-- 0. Environment assertions
-- ---------------------------------------------------------------------------

-- Which of the 16 names covered by IvoryOidByTypname this instance provides.
-- typtype: b=base type, d=domain. raw/long_raw are domains in 1.22, same branch.
-- ABSENT types have no column in this suite; add them on versions that do.
SELECT v.name,
       coalesce(t.typtype::text, '-') AS typtype,
       CASE WHEN t.oid IS NULL THEN 'ABSENT' ELSE 'present' END AS status
FROM (VALUES ('oradate'),('oratimestamp'),('oratimestamptz'),('oratimestampltz'),
             ('yminterval'),('dsinterval'),('number'),('binary_float'),
             ('binary_double'),('oravarcharchar'),('oravarcharbyte'),
             ('oracharchar'),('oracharbyte'),('raw'),('long_raw'),('xmltype')) v(name)
LEFT JOIN pg_type t
       ON t.typname = v.name
      AND t.typnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'sys')
ORDER BY v.name;

-- ---------------------------------------------------------------------------
-- 1. Test data
--    17 columns cover 15 Oracle types; number covers three typmod shapes,
--    because the typmod decides between the DECIMAL(p,s) and DOUBLE branches.
-- ---------------------------------------------------------------------------

CREATE TABLE ora_t (
  id         number(10) PRIMARY KEY,              -- number, precision no scale
  c_date     date,                                -- oradate
  c_ts       timestamp,                           -- oratimestamp
  c_tstz     timestamp with time zone,            -- oratimestamptz
  c_tsltz    timestamp with local time zone,      -- oratimestampltz
  c_ymint    interval year to month,              -- yminterval
  c_dsint    interval day to second,              -- dsinterval
  c_num      number(10,2),                        -- number -> DECIMAL(10,2)
  c_num_bare number,                              -- number, no typmod -> DOUBLE
  c_num_int  number(10),                          -- number -> DECIMAL(10,0)
  c_bfloat   binary_float,                        -- binary_float
  c_bdouble  binary_double,                       -- binary_double
  c_vc_char  varchar2(50 char),                   -- oravarcharchar
  c_vc_byte  varchar2(50),                        -- oravarcharbyte
  c_ch_char  char(10 char),                       -- oracharchar
  c_ch_byte  char(10),                            -- oracharbyte
  c_raw      raw(16),                             -- raw (domain)
  c_longraw  long raw                             -- long_raw (domain)
);

INSERT INTO ora_t
SELECT g,
       DATE '2024-01-01' + mod(g,365),
       TIMESTAMP '2024-01-01 00:00:00' + mod(g,365),
       TIMESTAMP '2024-01-01 00:00:00 +00:00' + mod(g,365),
       TIMESTAMP '2024-01-01 00:00:00' + mod(g,365),
       numtoyminterval(mod(g,24), 'month'),
       numtodsinterval(mod(g,86400), 'second'),
       mod(g,5000) + 1000.25,
       mod(g,5000) + 0.5,
       mod(g,5000) + 1000,
       mod(g,100) + 0.5,
       mod(g,100) + 0.25,
       'row_' || g,
       'row_' || g,
       'r' || mod(g,9),
       'r' || mod(g,9),
       hextoraw(lpad(to_hex(mod(g,256)), 4, '0')),
       hextoraw(lpad(to_hex(mod(g,256)), 8, '0'))
FROM generate_series(1,200) g;

-- Landed column types = input of ConvertPostgresToDuckColumnType
SELECT a.attname, t.typname
FROM pg_attribute a
JOIN pg_type t ON t.oid = a.atttypid
WHERE a.attrelid = 'ora_t'::regclass AND a.attnum > 0
ORDER BY a.attnum;

-- ---------------------------------------------------------------------------
-- 2. Execution engine + native baseline
--
-- mc_engine only runs EXPLAIN, never executes the plan, has no EXCEPTION block,
-- hence opens no subtransaction. mc_native evaluates under
-- force_execution=false, pure Postgres, and cannot throw. Both require
-- duckdb.unsafe_allow_execution_inside_functions = true (SET at the top).
-- ---------------------------------------------------------------------------

CREATE FUNCTION mc_engine(p_q text) RETURNS text
LANGUAGE plpgsql AS $fn$
DECLARE l text; hit boolean := false;
BEGIN
    PERFORM set_config('duckdb.force_execution', 'true', false);
    FOR l IN EXECUTE 'EXPLAIN (COSTS OFF) ' || p_q LOOP
        IF l LIKE '%DuckDBScan%' THEN hit := true; END IF;
    END LOOP;
    PERFORM set_config('duckdb.force_execution', 'false', false);
    RETURN CASE WHEN hit THEN 'duckdb' ELSE 'postgres' END;
END
$fn$;

CREATE FUNCTION mc_native(p_q text) RETURNS text
LANGUAGE plpgsql AS $fn$
DECLARE r text;
BEGIN
    PERFORM set_config('duckdb.force_execution', 'false', false);
    EXECUTE 'SELECT (t)::text FROM (' || p_q || ') t' INTO r;
    RETURN coalesce(r, '(null)');
END
$fn$;

CREATE TABLE mc_q (seq int, label text, q text);
INSERT INTO mc_q VALUES
 ( 1, 'value/all-but-bare',   $$SELECT id, c_date, c_ts, c_tstz, c_tsltz, c_ymint, c_dsint, c_num, c_num_int, c_bfloat, c_bdouble, c_vc_char, c_vc_byte, c_ch_char, c_ch_byte, c_raw, c_longraw FROM ora_t WHERE id = 7$$),

 (10, 'filter/oradate',         $$SELECT count(*) FROM ora_t WHERE c_date  >= DATE '2024-06-01'$$),
 (11, 'filter/oratimestamp',    $$SELECT count(*) FROM ora_t WHERE c_ts    >= TIMESTAMP '2024-06-01 00:00:00'$$),
 (12, 'filter/oratimestamptz',  $$SELECT count(*) FROM ora_t WHERE c_tstz  >= TIMESTAMP '2024-06-01 00:00:00 +00:00'$$),
 (13, 'filter/oratimestampltz', $$SELECT count(*) FROM ora_t WHERE c_tsltz >= TIMESTAMP '2024-06-01 00:00:00'$$),
 (14, 'filter/yminterval',      $$SELECT count(*) FROM ora_t WHERE c_ymint >= numtoyminterval(6, 'month')$$),
 (15, 'filter/dsinterval',      $$SELECT count(*) FROM ora_t WHERE c_dsint >= numtodsinterval(100, 'second')$$),
 (16, 'filter/number(10,2)',    $$SELECT count(*) FROM ora_t WHERE c_num      > 3000$$),
 (17, 'filter/number bare',     $$SELECT count(*) FROM ora_t WHERE c_num_bare > 100$$),
 (18, 'filter/number(10)',      $$SELECT count(*) FROM ora_t WHERE c_num_int  > 1100$$),
 (19, 'filter/binary_float',    $$SELECT count(*) FROM ora_t WHERE c_bfloat   > 50$$),
 (20, 'filter/binary_double',   $$SELECT count(*) FROM ora_t WHERE c_bdouble  > 50$$),
 (21, 'filter/oravarcharchar',  $$SELECT count(*) FROM ora_t WHERE c_vc_char = 'row_7'$$),
 (22, 'filter/oravarcharbyte',  $$SELECT count(*) FROM ora_t WHERE c_vc_byte = 'row_7'$$),
 (23, 'filter/oracharchar',     $$SELECT count(*) FROM ora_t WHERE c_ch_char = 'r7'$$),
 (24, 'filter/oracharbyte',     $$SELECT count(*) FROM ora_t WHERE c_ch_byte = 'r7'$$),
 (25, 'filter/raw',             $$SELECT count(*) FROM ora_t WHERE c_raw = hextoraw('0007')$$),
 (26, 'filter/long_raw',        $$SELECT count(*) FROM ora_t WHERE c_longraw <> c_raw$$),
 (27, 'filter/IN list',         $$SELECT count(*) FROM ora_t WHERE c_num_int IN (1100, 1200, 1300)$$),
 (28, 'filter/BETWEEN date',    $$SELECT count(*) FROM ora_t WHERE c_date BETWEEN DATE '2024-03-01' AND DATE '2024-06-01'$$),
 (29, 'filter/IS NULL',         $$SELECT count(*) FROM ora_t WHERE c_date IS NULL$$),

 (40, 'arith/number(10,2) *',   $$SELECT round(sum(c_num * 1.1 - 500)::numeric, 2)  FROM ora_t$$),
 (41, 'arith/number bare *',    $$SELECT round(sum(c_num_bare * 2)::numeric, 2)     FROM ora_t$$),
 (42, 'arith/number(10) *',     $$SELECT round(sum(c_num_int * 3)::numeric, 2)      FROM ora_t$$),
 (43, 'arith/number /',         $$SELECT round(sum(c_num / 7)::numeric, 2)          FROM ora_t$$),
 (44, 'arith/binary_float',     $$SELECT round(sum(c_bfloat * 1.5)::numeric, 2)     FROM ora_t$$),
 (45, 'arith/binary_double',    $$SELECT round(sum(c_bdouble * 1.5)::numeric, 2)    FROM ora_t$$),
 (46, 'arith/date + n',         $$SELECT count(*) FROM ora_t WHERE c_date + 1 > c_date$$),
 (47, 'arith/date - date',      $$SELECT count(*) FROM ora_t WHERE c_date - DATE '2024-01-01' >= 0$$),
 (48, 'arith/ts + dsinterval',  $$SELECT count(*) FROM ora_t WHERE c_ts + c_dsint >= c_ts$$),
 (49, 'arith/mixed num',        $$SELECT round(sum(c_num + c_bdouble)::numeric, 2)  FROM ora_t$$),

 (60, 'like/oravarcharbyte',    $$SELECT count(*) FROM ora_t WHERE c_vc_byte LIKE 'row_1%'$$),
 (61, 'like/oravarcharchar',    $$SELECT count(*) FROM ora_t WHERE c_vc_char LIKE 'row_1%'$$),
 (62, 'like/oracharbyte',       $$SELECT count(*) FROM ora_t WHERE c_ch_byte LIKE 'r%'$$),
 (63, 'like/oracharchar',       $$SELECT count(*) FROM ora_t WHERE c_ch_char LIKE 'r%'$$),
 (64, 'like/NOT LIKE',          $$SELECT count(*) FROM ora_t WHERE c_vc_byte NOT LIKE 'row_1%'$$),
 (65, 'like/upper()',           $$SELECT count(*) FROM ora_t WHERE upper(c_vc_byte) LIKE 'ROW_1%'$$),
 (66, 'like/substr()',          $$SELECT count(*) FROM ora_t WHERE substr(c_vc_byte, 1, 4) = 'row_'$$),
 (67, 'like/concat',            $$SELECT count(*) FROM ora_t WHERE c_vc_byte || 'x' LIKE 'row_1%x'$$),

 (80, 'agg/count+sum+avg',      $$SELECT count(*)||' '||sum(c_num)||' '||round(avg(c_num),4) FROM ora_t$$),
 (81, 'agg/min+max date',       $$SELECT min(c_date)||' '||max(c_date) FROM ora_t$$),
 (82, 'agg/group by char',      $$SELECT string_agg(g, ',' ORDER BY g) FROM (SELECT c_ch_byte||':'||count(*) g FROM ora_t GROUP BY c_ch_byte) s$$),
 (83, 'agg/group by date',      $$SELECT count(*) FROM (SELECT c_date, count(*) FROM ora_t GROUP BY c_date) s$$),
 (84, 'agg/order by limit',     $$SELECT string_agg(id::text, ',') FROM (SELECT id FROM ora_t ORDER BY c_num DESC, id LIMIT 5) s$$),
 (85, 'agg/window rank',        $$SELECT string_agg(id::text, ',') FROM (SELECT id, rank() OVER (ORDER BY c_num DESC, id) rk FROM ora_t) s WHERE rk <= 5$$),
 (86, 'agg/distinct',           $$SELECT count(DISTINCT c_ch_byte) FROM ora_t$$),
 (87, 'agg/self join',          $$SELECT count(*) FROM ora_t a JOIN ora_t b ON a.id = b.id WHERE a.c_num > 3000$$),
 (88, 'agg/date_trunc',         $$SELECT count(*) FROM (SELECT date_trunc('month', c_ts) m, count(*) FROM ora_t GROUP BY 1) s$$);

-- One table: which checks ran on DuckDB, and the native baseline values
SELECT label, mc_engine(q) AS engine, mc_native(q) AS native
FROM mc_q ORDER BY seq;

-- Fallback ratio: more postgres rows = less acceleration on Oracle types.
SELECT engine, count(*) AS checks
FROM (SELECT mc_engine(q) AS engine FROM mc_q) s
GROUP BY engine ORDER BY engine;

-- ---------------------------------------------------------------------------
-- 3-7. Accelerated-path measurements
--      Everything below runs bare under force_execution = true; compare the
--      output against the native column above. An ERROR is a hard failure.
-- ---------------------------------------------------------------------------

SET duckdb.force_execution = true;

-- value/all-but-bare
--
-- KNOWN CRASH - c_num_bare deliberately excluded:
-- projecting a typmod-less number through DuckDB segfaults the backend:
--
--     SET duckdb.force_execution = true;
--     SELECT c_num_bare FROM ora_t;      -- signal 11
--
-- The trigger is exactly the type_modifier == -1 branch of
-- pgduckdb_types.cpp: it returns DOUBLE + NumericAsDouble
-- (ExtraTypeInfoType::INVALID_TYPE_INFO) and then SetAlias("ivory:number").
-- The DECIMAL branches (number(10,2)/number(38,2)/number(10)) are all fine.
-- Filters and arithmetic do not crash - they fall back to Postgres at bind
-- time due to the alias mismatch, so DuckDB never reads the column.
--
-- The crash takes down the whole cluster and the suite cannot continue in the
-- same run, so the column is excluded here; its filter/arithmetic checks remain
SELECT id, c_date, c_ts, c_tstz, c_tsltz, c_ymint, c_dsint, c_num, c_num_int,
       c_bfloat, c_bdouble, c_vc_char, c_vc_byte, c_ch_char, c_ch_byte, c_raw, c_longraw
FROM ora_t WHERE id = 7;

-- filter/oradate
SELECT count(*) FROM ora_t WHERE c_date  >= DATE '2024-06-01';
-- filter/oratimestamp
SELECT count(*) FROM ora_t WHERE c_ts    >= TIMESTAMP '2024-06-01 00:00:00';
-- filter/oratimestamptz
SELECT count(*) FROM ora_t WHERE c_tstz  >= TIMESTAMP '2024-06-01 00:00:00 +00:00';
-- filter/oratimestampltz
SELECT count(*) FROM ora_t WHERE c_tsltz >= TIMESTAMP '2024-06-01 00:00:00';
-- filter/yminterval
SELECT count(*) FROM ora_t WHERE c_ymint >= numtoyminterval(6, 'month');
-- filter/dsinterval
SELECT count(*) FROM ora_t WHERE c_dsint >= numtodsinterval(100, 'second');
-- filter/number(10,2)
SELECT count(*) FROM ora_t WHERE c_num      > 3000;
-- filter/number bare
SELECT count(*) FROM ora_t WHERE c_num_bare > 100;
-- filter/number(10)
SELECT count(*) FROM ora_t WHERE c_num_int  > 1100;
-- filter/binary_float
SELECT count(*) FROM ora_t WHERE c_bfloat   > 50;
-- filter/binary_double
SELECT count(*) FROM ora_t WHERE c_bdouble  > 50;
-- filter/oravarcharchar
SELECT count(*) FROM ora_t WHERE c_vc_char = 'row_7';
-- filter/oravarcharbyte
SELECT count(*) FROM ora_t WHERE c_vc_byte = 'row_7';
-- filter/oracharchar
SELECT count(*) FROM ora_t WHERE c_ch_char = 'r7';
-- filter/oracharbyte
SELECT count(*) FROM ora_t WHERE c_ch_byte = 'r7';
-- filter/raw
SELECT count(*) FROM ora_t WHERE c_raw = hextoraw('0007');
-- filter/long_raw
SELECT count(*) FROM ora_t WHERE c_longraw <> c_raw;
-- filter/IN list
SELECT count(*) FROM ora_t WHERE c_num_int IN (1100, 1200, 1300);
-- filter/BETWEEN date
SELECT count(*) FROM ora_t WHERE c_date BETWEEN DATE '2024-03-01' AND DATE '2024-06-01';
-- filter/IS NULL
SELECT count(*) FROM ora_t WHERE c_date IS NULL;

-- arith/number(10,2) *
SELECT round(sum(c_num * 1.1 - 500)::numeric, 2)  FROM ora_t;
-- arith/number bare *
SELECT round(sum(c_num_bare * 2)::numeric, 2)     FROM ora_t;
-- arith/number(10) *
SELECT round(sum(c_num_int * 3)::numeric, 2)      FROM ora_t;
-- arith/number /
SELECT round(sum(c_num / 7)::numeric, 2)          FROM ora_t;
-- arith/binary_float
SELECT round(sum(c_bfloat * 1.5)::numeric, 2)     FROM ora_t;
-- arith/binary_double
SELECT round(sum(c_bdouble * 1.5)::numeric, 2)    FROM ora_t;
-- arith/date + n
SELECT count(*) FROM ora_t WHERE c_date + 1 > c_date;
-- arith/date - date
SELECT count(*) FROM ora_t WHERE c_date - DATE '2024-01-01' >= 0;
-- arith/ts + dsinterval
SELECT count(*) FROM ora_t WHERE c_ts + c_dsint >= c_ts;
-- arith/mixed num
SELECT round(sum(c_num + c_bdouble)::numeric, 2)  FROM ora_t;

-- like/oravarcharbyte
SELECT count(*) FROM ora_t WHERE c_vc_byte LIKE 'row_1%';
-- like/oravarcharchar
SELECT count(*) FROM ora_t WHERE c_vc_char LIKE 'row_1%';
-- like/oracharbyte
SELECT count(*) FROM ora_t WHERE c_ch_byte LIKE 'r%';
-- like/oracharchar
SELECT count(*) FROM ora_t WHERE c_ch_char LIKE 'r%';
-- like/NOT LIKE
SELECT count(*) FROM ora_t WHERE c_vc_byte NOT LIKE 'row_1%';
-- like/upper()
SELECT count(*) FROM ora_t WHERE upper(c_vc_byte) LIKE 'ROW_1%';
-- like/substr()
SELECT count(*) FROM ora_t WHERE substr(c_vc_byte, 1, 4) = 'row_';
-- like/concat
SELECT count(*) FROM ora_t WHERE c_vc_byte || 'x' LIKE 'row_1%x';

-- agg/count+sum+avg
SELECT count(*)||' '||sum(c_num)||' '||round(avg(c_num),4) FROM ora_t;
-- agg/min+max date
SELECT min(c_date)||' '||max(c_date) FROM ora_t;
-- agg/group by char
SELECT string_agg(g, ',' ORDER BY g) FROM (SELECT c_ch_byte||':'||count(*) g FROM ora_t GROUP BY c_ch_byte) s;
-- agg/group by date
SELECT count(*) FROM (SELECT c_date, count(*) FROM ora_t GROUP BY c_date) s;
-- agg/order by limit
SELECT string_agg(id::text, ',') FROM (SELECT id FROM ora_t ORDER BY c_num DESC, id LIMIT 5) s;
-- agg/window rank
SELECT string_agg(id::text, ',') FROM (SELECT id, rank() OVER (ORDER BY c_num DESC, id) rk FROM ora_t) s WHERE rk <= 5;
-- agg/distinct
SELECT count(DISTINCT c_ch_byte) FROM ora_t;
-- agg/self join
SELECT count(*) FROM ora_t a JOIN ora_t b ON a.id = b.id WHERE a.c_num > 3000;
-- agg/date_trunc
SELECT count(*) FROM (SELECT date_trunc('month', c_ts) m, count(*) FROM ora_t GROUP BY 1) s;

-- ---------------------------------------------------------------------------
-- 8. Round-trip type restoration
--    CTAS runs on the accelerated path; the landed table's column types must
--    match the source table column by column. This checks the DuckDB -> PG
--    direction: GetPostgresDuckDBType restoring oradate/number via ivory aliases.
-- ---------------------------------------------------------------------------

-- c_num_bare excluded here too (see the crash note above)
CREATE TABLE ora_rt AS
SELECT id, c_date, c_ts, c_tstz, c_tsltz, c_ymint, c_dsint, c_num, c_num_int,
       c_bfloat, c_bdouble, c_vc_char, c_vc_byte, c_ch_char, c_ch_byte, c_raw, c_longraw
FROM ora_t;

SET duckdb.force_execution = false;

SELECT a.attname,
       src.typname AS src_type,
       dst.typname AS ctas_type,
       CASE WHEN src.typname = dst.typname THEN 'ok' ELSE 'TYPE-DRIFT' END AS verdict
FROM pg_attribute a
JOIN pg_type dst ON dst.oid = a.atttypid
JOIN pg_attribute sa ON sa.attrelid = 'ora_t'::regclass AND sa.attname = a.attname
JOIN pg_type src ON src.oid = sa.atttypid
WHERE a.attrelid = 'ora_rt'::regclass AND a.attnum > 0
ORDER BY a.attnum;

-- Row count must match as well
SELECT (SELECT count(*) FROM ora_rt) AS ctas_rows,
       (SELECT count(*) FROM ora_t)  AS src_rows;

-- ---------------------------------------------------------------------------
-- Cleanup
-- ---------------------------------------------------------------------------

DROP TABLE ora_rt;
DROP TABLE mc_q;
DROP TABLE ora_t;
DROP FUNCTION mc_native(text);
DROP FUNCTION mc_engine(text);
RESET TIME ZONE;
RESET client_min_messages;
