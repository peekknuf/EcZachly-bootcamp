   
WITH changes_cte AS (
    SELECT 
        player_name,
        current_season,
        is_active,
        LAG(is_active) OVER (PARTITION BY player_name ORDER BY current_season) AS prev_is_active,
        years_since_last_active
    FROM players p
)

SELECT *,
    CASE
        WHEN prev_is_active IS NULL THEN 'New'
        WHEN prev_is_active = TRUE AND is_active = FALSE THEN 'Retired'
        WHEN prev_is_active = FALSE AND is_active = TRUE THEN 'Returned from Retirement'
        WHEN prev_is_active = TRUE AND is_active = TRUE THEN 'Continued Playing'
        WHEN prev_is_active = FALSE AND is_active = FALSE THEN 'Stayed Retired'
        ELSE 'Unknown'
    END AS state_change
FROM changes_cte;
