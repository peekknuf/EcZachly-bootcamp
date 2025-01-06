-- Insert into the `host_activity_reduced` table
WITH cte_dupes_identified AS (
    -- Identify duplicate events by assigning a row number to each event
    -- Events are partitioned by URL, referrer, user_id, host, and event_time
    SELECT
        *,
        ROW_NUMBER()
            OVER (
                PARTITION BY
                    url,
                    referrer,
                    user_id,
                    host,
                    event_time
            )
        AS checking_dupes  -- Row number to identify duplicates
    FROM events AS e
),
cte_prepared_events AS (
    -- Select only the deduplicated events (where checking_dupes = 1)
    SELECT
        user_id,
        host,
        CAST(event_time AS DATE) AS event_time  -- Truncate event_time to date
    FROM cte_dupes_identified
    WHERE checking_dupes = 1  -- Filter out duplicates
),
cte_yesterday AS (
    -- Fetch yesterday's cumulative data from `host_activity_reduced`
    SELECT *
    FROM host_activity_reduced
    WHERE "month" = DATE('2023-01-01')  -- Filter for the start of the month
),
cte_today AS (
    -- Prepare today's events by grouping by host and event date
    SELECT
        host,
        CAST(DATE_TRUNC('day', event_time) AS DATE) AS today_date,  -- Truncate event_time to today's date
        COUNT(1) AS num_hits,  -- Count the total number of hits per host
        COUNT(DISTINCT user_id) AS num_unique_visitors  -- Count the number of unique visitors per host
    FROM cte_prepared_events
    WHERE DATE_TRUNC('day', event_time) = '2023-01-02'  -- Filter for today's date
    GROUP BY host, DATE_TRUNC('day', event_time)
)

-- Combine yesterday's and today's data to update the cumulative arrays
SELECT
    COALESCE(y.host, t.host) AS host,  -- Use today's host or fallback to yesterday's
    DATE('2023-01-01') AS month_start,  -- Set the month start date
    -- Append today's hit count to the cumulative hit_array (or keep yesterday's array if no new data)
    COALESCE(y.hit_array, ARRAY[]::INTEGER[])
        || ARRAY[t.num_hits] AS hit_array,
    -- Append today's unique visitor count to the cumulative unique_visitors array (or keep yesterday's array if no new data)
    COALESCE(y.unique_visitors, ARRAY[]::INTEGER[])
        || ARRAY[t.num_unique_visitors] AS unique_visitors
FROM cte_yesterday y
FULL OUTER JOIN cte_today t
    ON y.host = t.host;  -- Join on host to combine yesterday's and today's data