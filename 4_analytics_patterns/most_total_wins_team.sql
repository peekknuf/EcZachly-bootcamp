WITH game_stats AS (
    SELECT
        gd.player_id,
        gd.player_name,
        gd.team_id,
        gd.team_abbreviation,
        g.season,
        COALESCE(gd.pts, 0) AS points_scored,
        CASE
            WHEN g.home_team_id = gd.team_id AND g.home_team_wins = 1 THEN 1
            WHEN g.visitor_team_id = gd.team_id AND g.home_team_wins = 0 THEN 1
            ELSE 0
        END AS team_won
    FROM game_details gd
    JOIN games g ON gd.game_id = g.game_id
)
SELECT
    CASE
        WHEN GROUPING(player_name) = 0 AND GROUPING(team_abbreviation) = 0 THEN 'player_team'
        WHEN GROUPING(player_name) = 0 AND GROUPING(season) = 0 THEN 'player_season'
        WHEN GROUPING(team_abbreviation) = 0 THEN 'team'
    END AS aggregation_level,
    COALESCE(player_name, '(overall)') AS player_name,
    COALESCE(team_abbreviation, '(overall)') AS team_name,
    COALESCE(season, NULL) AS season,
    SUM(points_scored) AS total_points,
    SUM(team_won) AS total_wins
FROM game_stats
GROUP BY GROUPING SETS (
    (player_name, team_abbreviation),  -- Player and team aggregation
    (player_name, season),             -- Player and season aggregation
    (team_abbreviation)                -- Team aggregation
)
ORDER BY total_wins DESC;
