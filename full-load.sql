--still have to figure out how to remove the unnecessary trailing data of years since last movie
--hehe full table insert goes brrrr

insert into actors
WITH years AS (
    SELECT generate_series(
        (SELECT MIN("year") FROM actor_films), 
        (SELECT MAX("year") FROM actor_films)
    ) AS current_year
),
p AS (
    SELECT actor, MIN("year") AS first_year
    FROM actor_films
    GROUP BY actor
),
actors_and_years AS (
    SELECT *
    FROM p
    CROSS JOIN years y
    WHERE p.first_year <= y.current_year
),
windowed AS (
    SELECT 
        af.actor,
        af.current_year,
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
    SELECT 
        actor, 
        MAX(actorid) AS actorid
    FROM actor_films
    GROUP BY actor
)
SELECT 
    w.actor,
    s.actorid,
    w.seasons AS films,
    CASE 
        WHEN COALESCE((w.seasons[array_length(w.seasons, 1)]).rating, 0) > 8 THEN 'star'
        WHEN COALESCE((w.seasons[array_length(w.seasons, 1)]).rating, 0) > 7 THEN 'good'
        WHEN COALESCE((w.seasons[array_length(w.seasons, 1)]).rating, 0) > 6 THEN 'average'
        ELSE 'bad'
    END::quality_class AS quality_class,
    w.current_year - COALESCE((w.seasons[array_length(w.seasons, 1)])."year", w.current_year) AS years_since_last_film,
    COALESCE((w.seasons[array_length(w.seasons, 1)])."year", NULL) = w.current_year AS is_active,
    w.current_year
FROM windowed w
JOIN static s ON w.actor = s.actor
group by s.actorid, w.actor,w.seasons, w.current_year;

