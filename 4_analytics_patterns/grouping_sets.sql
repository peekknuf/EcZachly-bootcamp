-- Define a Common Table Expression (CTE) named `game_stats` to preprocess game and player data
WITH game_stats AS (
    SELECT
        gd.player_id,                  -- Unique identifier for the player
        gd.player_name,                -- Name of the player
        gd.team_id,                    -- Unique identifier for the team
        gd.team_abbreviation,          -- Abbreviated name of the team
        g.season,                      -- Season in which the game was played
        COALESCE(gd.pts, 0) AS points_scored,  -- Points scored by the player, defaulting to 0 if NULL
        CASE
            -- Determine if the player's team won the game
            WHEN g.home_team_id = gd.team_id AND g.home_team_wins = 1 THEN 1  -- Home team won
            WHEN g.visitor_team_id = gd.team_id AND g.home_team_wins = 0 THEN 1  -- Visitor team won
            ELSE 0  -- Team lost
        END AS team_won
    FROM game_details gd  -- Table containing player-specific game details
    JOIN games g ON gd.game_id = g.game_id  -- Join with the games table to get game-level information
)

-- Main query to aggregate and analyze the preprocessed data
SELECT
    -- Determine the level of aggregation based on the grouping
    CASE
        WHEN GROUPING(player_name) = 0 AND GROUPING(team_abbreviation) = 0 THEN 'player_team'  -- Aggregation by player and team
        WHEN GROUPING(player_name) = 0 AND GROUPING(season) = 0 THEN 'player_season'  -- Aggregation by player and season
        WHEN GROUPING(team_abbreviation) = 0 THEN 'team'  -- Aggregation by team only
    END AS aggregation_level,  -- Label for the level of aggregation
    COALESCE(player_name, '(overall)') AS player_name,  -- Player name, defaulting to '(overall)' if NULL
    COALESCE(team_abbreviation, '(overall)') AS team_name,  -- Team name, defaulting to '(overall)' if NULL
    COALESCE(season, NULL) AS season,  -- Season, defaulting to NULL if not applicable
    SUM(points_scored) AS total_points,  -- Total points scored at the specified aggregation level
    SUM(team_won) AS total_wins  -- Total wins at the specified aggregation level
FROM game_stats  -- Use the preprocessed data from the CTE
-- Group the data using GROUPING SETS to create multiple levels of aggregation
GROUP BY GROUPING SETS (
    (player_name, team_abbreviation),  -- Aggregation by player and team
    (player_name, season),             -- Aggregation by player and season
    (team_abbreviation)                -- Aggregation by team only
)
-- Order the results by aggregation level and total points in descending order
ORDER BY aggregation_level, total_points DESC;
