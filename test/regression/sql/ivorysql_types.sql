-- Regression test: IvorySQL type fallback on standard PostgreSQL.
-- On standard PG, ivory OID lookups return InvalidOid and no ivory aliases
-- are set, so all ivory code paths are skipped. This test verifies that
-- TIMESTAMP/TIMESTAMPTZ/INTERVAL/FLOAT/DOUBLE/NUMERIC/VARCHAR/BYTEA types
-- work correctly after the new ivory guard code is added.
-- NOTE: This test covers only the standard-PG fallback path (guard code).
-- All ivory type conversion logic (Path A/C/D) requires an IvorySQL instance
-- with the sys schema and is not exercised by this test.

CREATE TABLE ivory_regression (
    ts   TIMESTAMP,
    tstz TIMESTAMPTZ,
    iv   INTERVAL,
    f4   FLOAT4,
    f8   FLOAT8,
    vc   TEXT,
    ba   BYTEA,
    num  NUMERIC(10,2)
);

INSERT INTO ivory_regression VALUES (
    '2024-03-15 12:34:56',
    '2024-03-15 12:34:56+00',
    '1 year 2 months 3 days',
    3.14::float4,
    2.71828,
    'hello',
    '\xDEADBEEF',
    123.45
);

-- Verify DuckDB execution returns correct types (no regression)
SET TIME ZONE 'UTC';
SET duckdb.force_execution = true;

SELECT ts, tstz, iv, f4, f8, vc, ba, num FROM ivory_regression;

SELECT pg_typeof(ts), pg_typeof(tstz), pg_typeof(iv), pg_typeof(f4),
       pg_typeof(f8), pg_typeof(vc), pg_typeof(ba), pg_typeof(num)
FROM ivory_regression;

SET duckdb.force_execution = false;
RESET TIME ZONE;

DROP TABLE ivory_regression;

-- ============================================================
-- Extended regression tests for IvorySQL guard/fallback path
-- ============================================================

-- 1. NULL and boundary values
CREATE TABLE ivory_boundary (
    ts   TIMESTAMP,
    tstz TIMESTAMPTZ,
    iv   INTERVAL,
    f4   FLOAT4,
    f8   FLOAT8,
    vc   TEXT,
    ba   BYTEA,
    num  NUMERIC(10,2)
);

INSERT INTO ivory_boundary VALUES
    (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
    ('1970-01-01 00:00:00', '1970-01-01 00:00:00+00', '0', 0, 0, '', '\x', 0),
    ('Infinity', 'Infinity', '1000 years', 3.4028235e+38, 1.7976931348623157e+308, 'max', '\xFF', 99999999.99),
    ('-Infinity', '-Infinity', '-1000 years', -3.4028235e+38, -1.7976931348623157e+308, 'min', '\x00', -99999999.99);

SET duckdb.force_execution = true;
SELECT * FROM ivory_boundary ORDER BY vc NULLS FIRST;
SELECT COUNT(*), COUNT(ts), COUNT(vc) FROM ivory_boundary;
SELECT MIN(ts), MAX(ts), MIN(f4), MAX(f8), MIN(num), MAX(num) FROM ivory_boundary;
SET duckdb.force_execution = false;

DROP TABLE ivory_boundary;

-- 2. Complex queries with multiple rows
CREATE TABLE ivory_complex_a (
    id INT PRIMARY KEY,
    ts TIMESTAMP,
    num NUMERIC(10,2),
    vc TEXT
);

CREATE TABLE ivory_complex_b (
    id INT PRIMARY KEY,
    fk INT,
    f8 FLOAT8,
    ba BYTEA
);

INSERT INTO ivory_complex_a VALUES
    (1, '2024-01-01 00:00:00', 100.50, 'alpha'),
    (2, '2024-06-15 12:30:00', 200.75, 'beta'),
    (3, '2024-12-31 23:59:59', 300.25, 'gamma');

INSERT INTO ivory_complex_b VALUES
    (10, 1, 1.1, '\xAA'),
    (20, 1, 2.2, '\xBB'),
    (30, 2, 3.3, '\xCC'),
    (40, 3, 4.4, '\xDD');

SET duckdb.force_execution = true;
-- JOIN
SELECT a.id, a.vc, b.f8 FROM ivory_complex_a a JOIN ivory_complex_b b ON a.id = b.fk ORDER BY a.id, b.id;
-- Aggregation
SELECT a.vc, COUNT(*), SUM(b.f8), AVG(b.f8) FROM ivory_complex_a a JOIN ivory_complex_b b ON a.id = b.fk GROUP BY a.vc ORDER BY a.vc;
-- Subquery
SELECT * FROM ivory_complex_a WHERE num > (SELECT AVG(num) FROM ivory_complex_a);
-- CTE
WITH summary AS (SELECT fk, SUM(f8) AS total FROM ivory_complex_b GROUP BY fk)
SELECT a.vc, s.total FROM ivory_complex_a a JOIN summary s ON a.id = s.fk ORDER BY a.vc;
SET duckdb.force_execution = false;

DROP TABLE ivory_complex_a;
DROP TABLE ivory_complex_b;

-- 3. Numeric precision tests
CREATE TABLE ivory_numeric (
    n1 NUMERIC(4,2),
    n2 NUMERIC(9,6),
    n3 NUMERIC(18,12),
    n4 NUMERIC(38,24),
    n5 NUMERIC
);

INSERT INTO ivory_numeric VALUES
    (12.34, 123.456789, 123456789.123456789012, 123456789012345678901234567890.123456789012345678901234, 12345678901234567890.1234567890);

SET duckdb.force_execution = true;
SELECT * FROM ivory_numeric;
SELECT pg_typeof(n1), pg_typeof(n2), pg_typeof(n3), pg_typeof(n4), pg_typeof(n5) FROM ivory_numeric;
SET duckdb.force_execution = false;

DROP TABLE ivory_numeric;

-- 4. Expression and cast tests
CREATE TABLE ivory_expr (
    f4 FLOAT4,
    f8 FLOAT8,
    num NUMERIC(10,2),
    vc TEXT,
    ts TIMESTAMP
);

INSERT INTO ivory_expr VALUES
    (3.14, 2.718, 100.50, 'hello', '2024-03-15 12:00:00');

SET duckdb.force_execution = true;
SELECT f4 + f8, f4 * num, num / 2 FROM ivory_expr;
SELECT vc || ' world', LENGTH(vc), UPPER(vc) FROM ivory_expr;
SELECT ts + INTERVAL '1 day', ts - INTERVAL '1 hour' FROM ivory_expr;
SELECT CAST(f8 AS NUMERIC(10,4)), CAST(num AS FLOAT8), CAST(vc AS VARCHAR(10)) FROM ivory_expr;
SET duckdb.force_execution = false;

DROP TABLE ivory_expr;

-- 5. DuckDB vs PG execution consistency
CREATE TABLE ivory_consistency (
    id INT,
    ts TIMESTAMP,
    tstz TIMESTAMPTZ,
    iv INTERVAL,
    f4 FLOAT4,
    f8 FLOAT8,
    vc TEXT,
    ba BYTEA,
    num NUMERIC(10,2)
);

INSERT INTO ivory_consistency VALUES
    (1, '2024-01-01 00:00:00', '2024-01-01 00:00:00+00', '1 day', 1.5, 2.5, 'a', '\x01', 10.50),
    (2, '2024-06-01 12:00:00', '2024-06-01 12:00:00+00', '2 months', 3.5, 4.5, 'b', '\x02', 20.50),
    (3, '2024-12-01 23:59:59', '2024-12-01 23:59:59+00', '1 year', 5.5, 6.5, 'c', '\x03', 30.50);

SET TIME ZONE 'UTC';
-- PG execution
SELECT * FROM ivory_consistency ORDER BY id;
-- DuckDB execution
SET duckdb.force_execution = true;
SELECT * FROM ivory_consistency ORDER BY id;
SET duckdb.force_execution = false;
RESET TIME ZONE;

DROP TABLE ivory_consistency;
