
WITH team_results AS (
    SELECT
        g.game_id,
        g.team_id_home AS team_id,
        g.pts_home AS points_scored,
        g.pts_away AS points_allowed,
        g.home_team_wins AS win_flag,
        ROW_NUMBER() OVER (PARTITION BY team_id_home ORDER BY g.game_date_est) AS game_num
    FROM games g
    UNION ALL
    SELECT
        g.game_id,
        g.team_id_away AS team_id,
        g.pts_away AS points_scored,
        g.pts_home AS points_allowed,
        1 - g.home_team_wins AS win_flag, -- Away team wins if home team loses
        ROW_NUMBER() OVER (PARTITION BY team_id_away ORDER BY g.game_date_est) AS game_num
    FROM games g
)
SELECT
    team_id,
    MAX(wins_in_90_games) AS most_wins_in_90_games
FROM (
    SELECT
        team_id,
        game_num,
        SUM(win_flag) OVER (
            PARTITION BY team_id
            ORDER BY game_num
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) AS wins_in_90_games
    FROM team_results
) subquery
GROUP BY team_id
ORDER BY most_wins_in_90_games DESC;
