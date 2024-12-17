
--DDL for fact table
create table fct_game_details (
	dim_game_date DATE,
	dim_season INTEGER,
	dim_team_id INTEGER,
	dim_player_id INTEGER,
	dim_player_name text,
	dim_start_position text,
	dim_is_playing_at_home BOOLEAN,
	dim_did_not_play BOOLEAN,
	dim_did_not_dress BOOLEAN,
	dim_not_with_team BOOLEAN,
	m_minutes real,
	m_fgm INTEGER,
	m_fga INTEGER,
	m_fg3m INTEGER,
	m_fg3a INTEGER,
	m_ftm INTEGER,
	m_fta INTEGER,
	m_oreb INTEGER,
	m_dreb INTEGER,
	m_reb INTEGER,
	m_ast INTEGER,
	m_stl INTEGER,
	m_blk INTEGER,
	m_turnovers INTEGER,
	m_pf INTEGER,
	m_pts INTEGER,
	m_plus_minus INTEGER,
	primary KEY(dim_game_date, dim_team_id, dim_player_id)
)






--data prep + ingestion into fact table
insert into fct_game_details
with deduped as (
	select g.game_date_est
			, g.season 
			, g.home_team_id 
			, gd.*
			, row_number () over (partition by gd.game_id, team_id, player_id) as dupes
			
	from game_details gd 
		join games g on gd.game_id = g.game_id 
)

select game_date_est as dim_game_date
		, season as dim_season
		, team_id as dim_team_id
		, player_id as dim_player_id
		, player_name as dim_player_name
		, start_position as dim_start_position
		, team_id = home_team_id as dim_is_playing_at_home
		, coalesce (position('DNP' in "comment"), 0) > 0 
			as dim_did_not_play
		, coalesce (position('DND' in "comment"), 0) > 0 
			as dim_did_not_dress
		, coalesce (position('NWT' in "comment"), 0) > 0 
			as dim_not_with_team
		, ROUND(
				    CAST(
				        (
				            CAST(SPLIT_PART("min", ':', 1) AS REAL) + 
				            CAST(SPLIT_PART("min", ':', 2) AS REAL) / 60
				        ) AS NUMERIC
				    ),
				    2
				) AS m_minutes
		, fgm as m_fgm
		, fga as m_fga
		, fg3m as m_fg3m
		, fg3a as m_fg3a
		, ftm as m_ftm
		, fta as m_fta
		, oreb as m_oreb
		, dreb as m_dreb
		, reb as m_reb
		, ast as m_ast
		, stl as m_stl
		, blk as m_blk
		, "TO"  as m_turnovers
		, pf as m_pf
		, pts  as m_pts
		, plus_minus as m_plus_minus
from deduped 
where dupes = 1;

