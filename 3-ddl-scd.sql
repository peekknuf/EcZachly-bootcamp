create table actors_history_scd (
	actor text,
	quality_class quality_class,
	is_active bool,
	start_year integer,
	end_year integer,
	current_year integer,
	primary key(actor, start_year)
);
