-- for each event there are 3 dupes
-- filtering them out
insert into user_devices_cumulated
with cte_events_deduped as (
    select
        *,
        d.device_id as device_ide,
        row_number()
            over (
                partition by
                    url,
                    referrer,
                    e.user_id,
                    d.device_id,
                    host,
                    event_time,
                    browser_type
            )
        as checking_dupes
    from events as e
    inner join devices as d
        on e.device_id = d.device_id
),
cte_prepared_events as (
    select
        user_id,
        device_ide as device_id,
        browser_type,
        cast(event_time as date) as event_time
    from cte_events_deduped where checking_dupes = 1
),
cte_yesterday as (
    select * from user_devices_cumulated where date = date('2023-01-07')
),
cte_today as (
    select
        device_id,
        browser_type,
        date_trunc('day', event_time)::date as today_date
    from cte_prepared_events
    where
        date_trunc('day', event_time) = date('2023-01-08')
    group by device_id, browser_type, date_trunc('day', event_time)
)

select
    coalesce(t.device_id, y.device_id) as device_id,
    coalesce(t.browser_type, y.browser_type) as browser_type,
    CAST(COALESCE(t.today_date, y."date" + INTERVAL '1 day') AS DATE) AS "date",
    COALESCE(y.device_activity_datelist,
           ARRAY[]::DATE[])
            || CASE WHEN
                t.device_id IS NOT NULL
                THEN ARRAY[t.today_date]
                ELSE ARRAY[]::DATE[]
                END AS date_list

from cte_yesterday as y
full outer join cte_today as t
    on
	y.device_id = t.device_id;
