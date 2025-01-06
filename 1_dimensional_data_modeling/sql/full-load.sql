-- Insert into the `actors` table
-- Note: This query still needs to address the issue of removing unnecessary trailing data for years since the last movie
-- Full table insert goes brrrr (massive insert operation)

insert into actors
WITH years AS (
    -- Generate a series of years from the earliest to the latest year in the actor_films table
    SELECT generate_series(
        (SELECT MIN("year") FROM actor_films),  -- Start year
        (SELECT MAX("year") FROM actor_films)   -- End year
    ) AS current_year
),
p AS (
    -- Find the first year each actor appeared in a film
    SELECT actor, MIN("year") AS first_year
    FROM actor_films
    GROUP BY actor
),
actors_and_years AS (
    -- Create a Cartesian product of actors and years, but only for years >= their first film year
    SELECT *
    FROM p
    CROSS JOIN years y
    WHERE p.first_year <= y.current_year
),
windowed AS (
    -- Aggregate films for each actor by year, using a window function to build an array of films
    SELECT
        af.actor,
        af.current_year,
        -- Build an array of films for each actor, removing NULL entries
        array_remove(
            array_agg(
                CASE
                    WHEN af1."year" IS NOT NULL THEN
                        ROW(
                            af1.filmid,
                            af1.film,
                            af1."year",
                            af1.votes,
                            af1.rating
                        )::films
                END
            ) OVER (
                PARTITION BY af.actor
                ORDER BY af.current_year
            ),
            NULL
        ) AS seasons
    FROM actors_and_years af
    LEFT JOIN actor_films af1
        ON af.actor = af1.actor AND af.current_year = af1."year"
    ORDER BY af.actor, af.current_year
),
static AS (
    -- Get the latest actorid for each actor (assuming actorid is unique per actor)
    SELECT
        actor,
        MAX(actorid) AS actorid
    FROM actor_films
    GROUP BY actor
)
-- Final select to prepare data for insertion into the `actors` table
SELECT
    w.actor,
    s.actorid,
    w.seasons AS films,  -- Array of films for the actor
    -- Determine the quality_class based on the rating of the most recent film
    CASE
        WHEN COALESCE((w.seasons[array_length(w.seasons, 1)]).rating, 0) > 8 THEN 'star'
        WHEN COALESCE((w.seasons[array_length(w.seasons, 1)]).rating, 0) > 7 THEN 'good'
        WHEN COALESCE((w.seasons[array_length(w.seasons, 1)]).rating, 0) > 6 THEN 'average'
        ELSE 'bad'
    END::quality_class AS quality_class,
    -- Calculate years since the last film (or 0 if no films exist)
    w.current_year - COALESCE((w.seasons[array_length(w.seasons, 1)])."year", w.current_year) AS years_since_last_film,
    -- Determine if the actor is active (i.e., had a film in the current year)
    COALESCE((w.seasons[array_length(w.seasons, 1)])."year", NULL) = w.current_year AS is_active,
    w.current_year
FROM windowed w
JOIN static s ON w.actor = s.actor
-- Group by to ensure unique rows per actor, year, and films
GROUP BY s.actorid, w.actor, w.seasons, w.current_year;