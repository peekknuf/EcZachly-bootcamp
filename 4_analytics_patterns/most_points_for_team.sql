
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
    player_name,
    team_abbreviation,
    SUM(points_scored) AS total_points
FROM game_stats
GROUP BY player_name, team_abbreviation  -- Aggregate by player and team
ORDER BY total_points DESC  -- Order by total points in descending order
LIMIT 1;  -- Get the player with the most points for a single team


