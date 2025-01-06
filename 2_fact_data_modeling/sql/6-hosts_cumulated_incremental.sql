-- Insert into the `hosts_cumulated` table
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
                    e.user_id,
                    host,
                    event_time
            )
        AS checking_dupes  -- Row number to identify duplicates
    FROM events AS e
),
cte_prepared_events AS (
    -- Select only the deduplicated events (where checking_dupes = 1)
    SELECT
        host,
        CAST(event_time AS DATE) AS event_time  -- Truncate event_time to date
    FROM cte_dupes_identified
    WHERE checking_dupes = 1  -- Filter out duplicates
),
cte_yesterday AS (
    -- Fetch yesterday's cumulative data from `hosts_cumulated`
    SELECT * FROM hosts_cumulated
    WHERE date = DATE('2023-01-02')  -- Filter for yesterday's date
),
cte_today AS (
    -- Prepare today's events by grouping by host and event date
    SELECT
        host,
        event_time AS days,  -- Truncate event_time to today's date
        COUNT(1) AS num_events  -- Count the number of events per host and date
    FROM cte_prepared_events
    WHERE
        event_time = '2023-01-03'  -- Filter for today's date
    GROUP BY 1, 2  -- Group by host and event date
),
cte_aggregated_json AS (
    -- Combine yesterday's and today's data to update the cumulative JSON object
    SELECT
        COALESCE(t.host, y.host) AS host,  -- Use today's host or fallback to yesterday's
        COALESCE(t.days, y."date" + INTERVAL '1 day') AS "date",  -- Set the date to today's date or increment yesterday's date by 1 day
        -- Append today's events to the cumulative JSON object (or keep yesterday's object if no new data)
        COALESCE(y.host_activity_datelist, '{}'::jsonb)
            || CASE
                WHEN t.host IS NOT NULL
                THEN JSONB_OBJECT_AGG(days::TEXT, num_events)  -- Append today's events as a JSON object
                ELSE '{}'::jsonb  -- Keep the JSON object unchanged if no new data
            END AS host_activity_datelist
    FROM cte_yesterday y
    FULL OUTER JOIN cte_today t
        ON t.host = y.host  -- Join on host to combine yesterday's and today's data
    GROUP BY t.host, y.host, t.days, y."date" + INTERVAL '1 day', y.host_activity_datelist
)

-- Insert the final result into the `hosts_cumulated` table
INSERT INTO hosts_cumulated
SELECT * FROM cte_aggregated_json;