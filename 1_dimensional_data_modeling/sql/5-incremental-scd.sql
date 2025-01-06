
-- Create a composite type `scd_type` to represent Slowly Changing Dimension (SCD) records
CREATE TYPE scd_type AS (
    quality_class quality_class,  -- Quality class of the actor
    is_active boolean,            -- Active status of the actor
    start_year INTEGER,           -- Start year of the SCD record
    end_year INTEGER              -- End year of the SCD record
);

-- Begin the main query to update actors_history_scd
WITH last_year_scd AS (
    -- Fetch SCD records from the previous year (2020)
    SELECT * FROM actors_history_scd
    WHERE current_year = 2020
),
historical_scd AS (
    -- Fetch historical SCD records from years before 2021
    SELECT
        actor,
        quality_class,
        is_active,
        start_year,
        end_year
    FROM actors_history_scd
    WHERE current_year = 2020
    AND current_year < 2021
),
this_year_data AS (
    -- Fetch current year (2021) data from the actors table
    SELECT * FROM actors
    WHERE current_year = 2021
),
unchanged_records AS (
    -- Identify records where there is no change in quality_class or is_active status
    SELECT
        ts.actor,
        ts.quality_class,
        ts.is_active,
        ls.start_year,            -- Retain the start_year from last_year_scd
        ts.current_year as end_season  -- Update the end_year to the current year
    FROM this_year_data ts
    JOIN last_year_scd ls
    ON ls.actor = ts.actor
    WHERE ts.quality_class = ls.quality_class
    AND ts.is_active = ls.is_active
),
changed_records AS (
    -- Identify records where there is a change in quality_class or is_active status
    SELECT
        ts.actor,
        -- Use UNNEST to create two rows for each changed record:
        -- 1. The old record (from last_year_scd)
        -- 2. The new record (from this_year_data)
        UNNEST(ARRAY[
            ROW(
                ls.quality_class,
                ls.is_active,
                ls.start_year,
                ls.end_year
            )::scd_type,  -- Old record
            ROW(
                ts.quality_class,
                ts.is_active,
                ts.current_year,
                ts.current_year
            )::scd_type   -- New record
        ]) as records
    FROM this_year_data ts
    LEFT JOIN last_year_scd ls
    ON ls.actor = ts.actor
    WHERE (ts.quality_class <> ls.quality_class
    OR ts.is_active <> ls.is_active)
),
unnested_changed_records AS (
    -- Unnest the changed_records to flatten the array into individual rows
    SELECT
        actor,
        (records::scd_type).quality_class,
        (records::scd_type).is_active,
        (records::scd_type).start_year,
        (records::scd_type).end_year
    FROM changed_records
),
new_records AS (
    -- Identify new records that do not exist in last_year_scd
    SELECT
        ts.actor,
        ts.quality_class,
        ts.is_active,
        ts.current_year AS start_year,  -- Set start_year to the current year
        ts.current_year AS end_year     -- Set end_year to the current year
    FROM this_year_data ts
    LEFT JOIN last_year_scd ls
    ON ts.actor = ls.actor
    WHERE ls.actor IS NULL  -- Only include actors not present in last_year_scd
)

-- Combine all records (historical, unchanged, changed, and new) into a single result set
SELECT *, 2021 AS current_year FROM (
    SELECT *
    FROM historical_scd

    UNION ALL

    SELECT *
    FROM unchanged_records

    UNION ALL

    SELECT *
    FROM unnested_changed_records

    UNION ALL

    SELECT *
    FROM new_records
) a;