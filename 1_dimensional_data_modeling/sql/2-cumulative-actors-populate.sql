-- Define a CTE (Common Table Expression) to fetch actors from the previous year (1969)
with last_year as (
    select * from actors
    where current_year = 1969
),

-- Define another CTE to fetch actor films for the current year (1970)
this_year as (
    select * from actor_films
    where year = 1970
)

-- Insert or update actors data for the current year (1970)
insert into actors
select
    -- Use the actor ID from either last_year or this_year, prioritizing existing data
    coalesce(ly.actorid, ty.actorid) as actorid,
    -- Use the actor name from either last_year or this_year, prioritizing existing data
    coalesce(ly.actor, ty.actor) as actor,
    -- Combine films from last_year with new films from this_year (if any)
    coalesce(
        ly.films,
        array[]::films [] -- Default to an empty array if no films exist in last_year
    ) || case
        -- Append a new film entry if a film exists in this_year
        when ty.year is not null
            then
                array[row(
                    ty.filmid,
                    ty.film,
                    ty.year,
                    ty.votes,
                    ty.rating
                )::films]
        else array[]::films [] -- Default to an empty array if no new film exists
    end as films,
    -- Determine the quality class based on the film rating in this_year
    case
        when ty.film is not null
            then
                (case
                    when ty.rating > 8 then 'star'
                    when ty.rating > 7 and ty.rating <= 8 then 'good'
                    when ty.rating > 6 and ty.rating <= 7 then 'average'
                    else 'bad'
                end)::quality_class
        else ly.quality_class -- Retain the quality class from last_year if no new film exists
    end as quality_class,
    -- Mark the actor as active if they have a film in this_year
    ty.year is not null as is_active,
    -- Set the current year to 1970 for all inserted/updated records
    1970 as current_year
from last_year as ly
-- Perform a full outer join to include all actors from last_year and this_year
full outer join this_year as ty on ly.actor = ty.actor;