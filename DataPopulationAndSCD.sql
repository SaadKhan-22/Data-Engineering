-- Custom Types for ARRAY Structs
CREATE TYPE films AS (
	film TEXT,
	votes INTEGER,
	rating REAL,
	filmid TEXT
)

CREATE TYPE quality_class AS ENUM('star', 'good', 'average', 'bad')

CREATE TABLE actors (
actor TEXT,
actorid TEXT,
year INTEGER, 
films films[],
quality_class quality_class,
is_active BOOLEAN
)


-- Incremental Data Population 

INSERT INTO actors 

with yesterday as(
select * from actors where film_year = '1974'
),

today as (
select * from actor_films where year = '1975'
)

SELECT
COALESCE(t.actor, y.actor) actor,
COALESCE(t.actorid, y.actorid) actorid,
-- COALESCE(y.actorid || '_' || y.films[1]::films || '_' || y.film_year, t.actorid || '_' || t.filmid || '_' || t.year) afy_id,
COALESCE(t.year, y.film_year) film_year,
CASE WHEN y.films IS NULL THEN ARRAY[ROW( t.filmid, t.film::text, t.votes, t.rating)::films]
WHEN t.film IS NOT NULL THEN y.films || ARRAY[ROW( t.filmid, t.film::text, t.votes, t.rating)::films]
ELSE y.films END AS films,
CASE WHEN t.year IS NOT NULL 
THEN
	CASE WHEN t.rating > 8 THEN 'star'
	WHEN t.rating > 7 AND t.rating <= 8 THEN 'good'
	WHEN t.rating > 6 AND t.rating <= 7 THEN 'average'
	WHEN t.rating <= 6 THEN 'average' END::quality_class
	ELSE y.quality_class END AS quality_class,
(CASE WHEN t.year IS NULL THEN 0 ELSE 1 END)::boolean AS is_active
FROM today as t
FULL OUTER JOIN yesterday as y
ON t.actor = y.actor;




-- SCD Using the Data Above

With previous AS (
SELECT
	actor, film_year, quality_class, is_active,
	LAG(quality_class, 1) OVER (PARTITION BY actor ORDER BY film_year) AS prev_quality_class,
	LAG(is_active, 1) OVER (PARTITION BY actor ORDER BY film_year) AS prev_is_active
from actors
where film_year <= 2006
),
indicators AS(

SELECT *,
	CASE WHEN quality_class <> prev_quality_class THEN 1
	WHEN is_active <> prev_is_active THEN 1 
	ELSE 0 END AS change_indicator
FROM previous
),
streaks AS (
SELECT *, SUM(change_indicator) OVER (PARTITION BY actor ORDER BY film_year) AS streak_identifier
FROM indicators
)

SELECT actor, streak_identifier, quality_class, is_active,
	MIN(film_year) AS start_year,
	MAX(film_year) AS end_year,
	2007 AS current_year -- only uses data until 2007
FROM streaks
GROUP BY actor, streak_identifier, quality_class, is_active
ORDER BY actor, streak_identifier;
