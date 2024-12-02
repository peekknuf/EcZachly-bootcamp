create table user_devices_cumulated (
	user_id numeric,
	device_id numeric,
	browser_type text,
	date date,
	device_activity_datelist date[],
	primary key(user_id, device_id, date)
);
