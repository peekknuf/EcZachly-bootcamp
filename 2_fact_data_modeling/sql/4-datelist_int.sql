-- Insert into the `datelist_int` table
WITH cte_devices AS (
    -- Fetch cumulative device activity data for the specific date '2023-01-31'
    SELECT * FROM user_devices_cumulated
    WHERE date = '2023-01-31'
),
cte_series AS (
    -- Generate a series of dates from '2022-12-31' to '2023-01-31' with a 1-day interval
    SELECT CAST(generate_series(DATE('2022-12-31'), DATE('2023-01-31'), INTERVAL '1 day') AS DATE) AS series_date
),
cte_crossjoined AS (
    -- Cross join the device data with the generated date series
    -- Check if each date in the series is present in the device's activity datelist
    SELECT
        d.device_activity_datelist @> ARRAY[s.series_date] AS is_active,  -- Check if the date is in the datelist
        (DATE '2023-01-31' - s.series_date) AS days_since,  -- Calculate the number of days since the series date
        device_id
    FROM cte_devices d
    CROSS JOIN cte_series s
),
cte_bits AS (
    -- Convert the active dates into a 32-bit integer representation
    SELECT
        device_id,
        SUM(
            CASE
                WHEN is_active THEN POW(2, 32 - days_since)  -- Set the bit if the date is active
                ELSE 0  -- Leave the bit unset if the date is inactive
            END
        )::BIGINT::BIT(32) AS datelist_int  -- Convert the sum to a 32-bit integer
    FROM cte_crossjoined
    GROUP BY device_id
)

-- Insert the final result into the `datelist_int` table
INSERT INTO datelist_int
SELECT * FROM cte_bits;