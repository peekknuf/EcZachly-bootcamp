-- Insert into the `user_devices_cumulated` table
-- Note: Each event has 3 duplicates, so we filter them out before processing
insert into user_devices_cumulated
WITH cte_events_deduped AS (
    -- Deduplicate events by assigning a row number to each event
    -- Events are partitioned by URL, referrer, user_id, device_id, host, event_time, and browser_type
    SELECT
        *,
        d.device_id AS device_ide,  -- Alias for device_id to avoid ambiguity
        ROW_NUMBER()
            OVER (
                PARTITION BY
                    url,
                    referrer,
                    e.user_id,
                    d.device_id,
                    host,
                    event_time,
                    browser_type
            )
        AS checking_dupes  -- Row number to identify duplicates
    FROM events AS e
    INNER JOIN devices AS d
        ON e.device_id = d.device_id
),
cte_prepared_events AS (
    -- Select only the deduplicated events (where checking_dupes = 1)
    SELECT
        user_id,
        device_ide AS device_id,  -- Use the deduplicated device_id
        browser_type,
        CAST(event_time AS DATE) AS event_time  -- Truncate event_time to date
    FROM cte_events_deduped
    WHERE checking_dupes = 1  -- Filter out duplicates
),
cte_yesterday AS (
    -- Fetch yesterday's cumulative data from `user_devices_cumulated`
    SELECT * FROM user_devices_cumulated
    WHERE date = DATE('2023-01-07')  -- Filter for yesterday's date
),
cte_today AS (
    -- Prepare today's events by grouping by device_id, browser_type, and event date
    SELECT
        device_id,
        browser_type,
        DATE_TRUNC('day', event_time)::DATE AS today_date  -- Truncate event_time to today's date
    FROM cte_prepared_events
    WHERE
        DATE_TRUNC('day', event_time) = DATE('2023-01-08')  -- Filter for today's date
    GROUP BY device_id, browser_type, DATE_TRUNC('day', event_time)
)

-- Combine yesterday's and today's data to update the cumulative table
SELECT
    COALESCE(t.device_id, y.device_id) AS device_id,  -- Use today's device_id or fallback to yesterday's
    COALESCE(t.browser_type, y.browser_type) AS browser_type,  -- Use today's browser_type or fallback to yesterday's
    -- Set the date to today's date or increment yesterday's date by 1 day
    CAST(COALESCE(t.today_date, y."date" + INTERVAL '1 day') AS DATE) AS "date",
    -- Append today's date to the cumulative date list (or keep yesterday's list if no new data)
    COALESCE(y.device_activity_datelist, ARRAY[]::DATE[])
        || CASE
            WHEN t.device_id IS NOT NULL
            THEN ARRAY[t.today_date]  -- Append today's date if there's new data
            ELSE ARRAY[]::DATE[]  -- Keep the list unchanged if no new data
        END AS date_list
FROM cte_yesterday AS y
FULL OUTER JOIN cte_today AS t
    ON y.device_id = t.device_id;  -- Join on device_id to combine yesterday's and today's data