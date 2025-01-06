-- Insert into actors_history_scd table (Slowly Changing Dimension for actors)
-- This query is optimized for millions of users but not for billions due to scalability constraints
insert into actors_history_scd

-- CTE: Fetch previous year's data for each actor, including quality_class and is_active status
with previous as (
    select actor,
           quality_class,
           current_year,
           is_active,
           -- Use LAG to get the previous year's quality_class for each actor
           LAG("quality_class", 1) over (partition by actor order by current_year) as previous_quality_class,
           -- Use LAG to get the previous year's is_active status for each actor
           LAG(is_active, 1) over (partition by actor order by current_year) as previous_is_active
    from actors
),

-- CTE: Identify changes in quality_class or is_active status using a change indicator
indicators as (
    select *,
           -- Create a change_indicator: 1 if there's a change in quality_class or is_active, else 0
           case when "quality_class" <> previous_quality_class then 1
                when is_active <> previous_is_active then 1
                else 0
           end as change_indicator
    from previous
),

-- CTE: Create streaks of consecutive years with no changes in quality_class or is_active
streaks as (
    select *,
           -- Use a running sum of change_indicator to create a unique streak_identifier for each period of no changes
           sum(change_indicator) over (partition by actor order by current_year) as streak_identifier
    from indicators
),

-- CTE: Aggregate streaks to determine the start and end years for each period of no changes
final_prep as (
    select actor,
           quality_class,
           is_active,
           streak_identifier,
           -- Find the first year of the streak
           min(current_year) as first_year,
           -- Find the last year of the streak
           max(current_year) as end_year
    from streaks
    -- Group by actor, streak_identifier, and other attributes to aggregate streaks
    group by actor, streak_identifier, is_active, quality_class
    order by actor, streak_identifier
)

-- Final select to prepare data for insertion into actors_history_scd
select actor,
       quality_class,
       is_active,
       first_year,
       end_year,
       -- Set the current_year to 2021 for all records (assumed to be the reporting year)
       2021 as current_year
from final_prep;