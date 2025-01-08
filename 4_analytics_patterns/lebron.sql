
WITH lebron_games AS (
    SELECT
        gd.game_id,
        gd.player_name,
        gd.pts,
        g.game_date_est,
        -- Use ROW_NUMBER to assign a unique sequential number to each game
        ROW_NUMBER() OVER (ORDER BY g.game_date_est) AS game_number
    FROM game_details gd
    JOIN games g ON gd.game_id = g.game_id
    WHERE gd.player_name = 'LeBron James' and gd.pts is not NULL  -- Filter for LeBron James
),
streaks AS (
    SELECT
        game_id,
        player_name,
        pts,
        game_number,
        -- Use a window function to calculate the cumulative streak of games with over 10 points
        SUM(CASE WHEN pts > 10 THEN 1 ELSE 0 END) OVER (ORDER BY game_number ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS streak
    FROM lebron_games
)
SELECT
    MAX(streak) AS longest_streak_over_10_points
FROM streaks;

