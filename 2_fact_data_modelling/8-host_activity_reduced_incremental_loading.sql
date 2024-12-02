
insert into host_activity_reduced
with cte_dupes_identified as (
 select
        *,
        row_number()
            over (
                partition by
                    url,
                    referrer,
                    user_id,
                    host,
                    event_time
            )
        as checking_dupes
    from events as e
    ),
cte_prepared_events as (
    select
        user_id,
        host,
        cast(event_time as date) as event_time
    from cte_dupes_identified where checking_dupes = 1
  ),
cte_yesterday as ( 
 select * 
 from host_activity_reduced 
 where "month" = date('2023-01-01')
),
cte_today as ( 
	select host,
			cast(date_trunc('day', event_time) as date) as today_date,
			count(1) as num_hits,
			count(distinct user_id) as num_unique_visitors
		from cte_prepared_events
		where date_trunc('day', event_time) = '2023-01-02'
		group by host, date_trunc('day', event_time)
)
select 
	coalesce (y.host, t.host) as host,
	DATE('2023-01-01') as month_start,
	coalesce (y.hit_array, array[]::integer[])
			|| array[t.num_hits] as hit_array,
	coalesce (y.unique_visitors, array[]::integer[])
			|| array[t.num_unique_visitors] as unique_visitors
from cte_yesterday y
	 full outer join cte_today t
	  on y.host = t.host;

