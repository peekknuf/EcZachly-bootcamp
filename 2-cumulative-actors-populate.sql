with last_year as (
    select * from actors
    where current_year = 1969
),

this_year as (
    select * from actor_films
    where year = 1970
)

insert into actors
select
    coalesce(ly.actorid, ty.actorid) as actorid,
    coalesce(ly.actor, ty.actor) as actor,
    coalesce(
        ly.films,
        array[]::films []
    ) || case
        when ty.year is not null
            then
                array[row(
                    ty.filmid,
                    ty.film,
                    ty.year,
                    ty.votes,
                    ty.rating
                )::films]
        else array[]::films []
    end as films,
    case
        when ty.film is not null
            then
                (case
                    when ty.rating > 8 then 'star'
                    when ty.rating > 7 and ty.rating <= 8 then 'good'
                    when ty.rating > 6 and ty.rating <= 7 then 'average'
                    else 'bad'
                end)::quality_class
        else ly.quality_class
    end as quality_class,
    ty.year is not null as is_active,
    1970 as current_year
from last_year as ly
full outer join this_year as ty on ly.actor = ty.actor;
