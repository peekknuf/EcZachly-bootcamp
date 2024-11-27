CREATE TYPE films AS (
    filmid TEXT,
    film TEXT,
    year_released integer,
    votes INTEGER,
    rating INTEGER
);

CREATE TYPE quality_class AS ENUM ('star', 'good', 'average', 'bad');

CREATE TABLE actors (
    actorid TEXT,
    actor TEXT,
    films FILMS [],
    quality_class QUALITY_CLASS,
    is_active BOOLEAN,
    current_year INTEGER,
    PRIMARY KEY (actor, films, current_year)
);
