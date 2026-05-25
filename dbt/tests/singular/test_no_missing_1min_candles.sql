-- =============================================================================
-- tests/singular/test_no_missing_1min_candles.sql
-- =============================================================================
-- TYPE: Singular Test (advanced - time series completeness check)
--
-- PURPOSE:
--   Detect gaps in the 1-minute candle time series.
--   Binance trades are highly active, so 1-min candles should be continuous.
--
-- SCOPE: Only checks the last 2 hours to avoid full table scan.
--
-- LOGIC:
--   1. Get active symbols (have data in last 2 hours)
--   2. Generate expected time series: 1 minute per row, 2 hours
--   3. LEFT JOIN with actual data -> NULL right side = missing minute
--
-- COLUMN CHANGES:
--   Old: window_start, window_minutes = 1
--   New: candle_time, candle_duration = '1 minute'
--
-- NOTE: This test will FAIL on mock/historical data. It is designed
-- for live/recent data only. Exclude via:
--   dbt test --exclude test_no_missing_1min_candles
-- =============================================================================

WITH
time_bounds AS (
    SELECT
        CAST(DATE_TRUNC('minute', CAST(NOW() AS timestamp)) - INTERVAL '2' HOUR AS timestamp) AS start_minute,
        CAST(DATE_TRUNC('minute', CAST(NOW() AS timestamp)) AS timestamp) AS end_minute
),

-- Step 1: Get active symbols with 1-min candles in the last 2 hours
active_symbols AS (
    SELECT DISTINCT symbol
    FROM {{ source('gold', 'gold_ohlcv') }}
    WHERE
        candle_duration = '1 minute'
        AND CAST(candle_time AS timestamp) >= (
            SELECT start_minute FROM time_bounds
        )
),

-- Step 2: Generate expected time series (1 row per minute, 2 hours)
expected_time_series AS (
    SELECT
        s.symbol,
        ts_table.expected_minute
    FROM active_symbols s
    CROSS JOIN time_bounds b
    CROSS JOIN UNNEST(
        SEQUENCE(
            b.start_minute,
            b.end_minute,
            INTERVAL '1' MINUTE
        )
    ) AS ts_table(expected_minute)
),

-- Step 3: Actual candles from Gold table
actual_candles AS (
    SELECT
        symbol,
        CAST(DATE_TRUNC('minute', CAST(candle_time AS timestamp)) AS timestamp) AS actual_minute
    FROM {{ source('gold', 'gold_ohlcv') }}
    WHERE
        candle_duration = '1 minute'
        AND CAST(candle_time AS timestamp) >= (
            SELECT start_minute FROM time_bounds
        )
)

-- Step 4: Find missing minutes via LEFT JOIN
SELECT
    e.symbol,
    e.expected_minute  AS missing_minute,
    'MISSING 1-min candle detected' AS test_message
FROM expected_time_series e
LEFT JOIN actual_candles a
    ON e.symbol = a.symbol
    AND e.expected_minute = a.actual_minute
WHERE
    a.actual_minute IS NULL
