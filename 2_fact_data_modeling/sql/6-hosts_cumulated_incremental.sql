with cte_dupes_identified as (
 select
        *,
        row_number()
            over (
                partition by
                    url,
                    referrer,
                    e.user_id,
                    host,
                    event_time
 
            )
        as checking_dupes
    from events as e
    ),
cte_prepared_events as (
    select
        host,
        cast(event_time as date) as event_time
    from cte_dupes_identified where checking_dupes = 1
  ),
cte_yesterday as (
    select * from hosts_cumulated where date = date('2023-01-02')
),
cte_today as (
    select
        host,
        event_time as days,
        count(1) as num_events
    from cte_prepared_events
    where
        event_time = '2023-01-03'
    group by 1,2
),
cte_aggregated_json AS (
    SELECT
        coalesce (t.host, y.host) as host,
        COALESCE(t.days, y."date" + INTERVAL '1 day') AS "date",
        COALESCE(y.host_activity_datelist,
           '{}'::jsonb)
            || CASE WHEN
                t.host IS NOT NULL
                THEN JSONB_OBJECT_AGG(days::TEXT, num_events)
                ELSE '{}'::jsonb
                END AS host_activity_datelist
    FROM cte_yesterday y
     full outer join cte_today t
      on t.host = y.host
    GROUP BY t.host, y.host, t.days, y."date" + INTERVAL '1 day',y.host_activity_datelist
)
insert into hosts_cumulated 
select * from cte_aggregated_json;
