
with cte_devices as (
		select * from user_devices_cumulated where date = '2023-01-31'
),
cte_series as ( 
		select cast(generate_series(date('2022-12-31'), date('2023-01-31'), interval '1 day') as date) as series_date
),
cte_crossjoined as ( 
		select  d.device_activity_datelist @> array[s.series_date] as is_active,
            (date '2023-01-31' - s.series_date) as days_since,
            device_id
		from cte_devices d 
			cross join cte_series s 
),
cte_bits as ( 
		select device_id,
		sum(case
			when is_active then pow(2,32 - days_since)
			else 0
		end)::bigint::bit(32) as datelist_int
		from cte_crossjoined
		group by device_id
		)

insert into datelist_int
select * from cte_bits

