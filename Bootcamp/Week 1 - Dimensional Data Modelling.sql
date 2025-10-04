



-- ********************************************************** Day 1  *********************************************************


-- -- select * from	public.player_seasons;

-- -- ALTER TABLE public.player_seasons

-- -- CREATE TYPE season_stats AS (
-- --     season INTEGER,
-- --     gp INTEGER,
-- --     pts REAL,
-- --     reb REAL,
-- -- 	ast REAL
-- -- )

-- CREATE TYPE scoring_class AS ENUM('star', 'good', 'average', 'bad');



-- CREATE TABLE players (
-- 	player_name TEXT,
-- 	height TEXT,
-- 	college TEXT,
-- 	country TEXT,
-- 	draft_year TEXT,
-- 	draft_round TEXT,
-- 	draft_number TEXT,
-- 	season_stats season_stats[],
-- 	scoring_class scoring_class,
-- 	years_since_last_season INTEGER,
-- 	current_season INTEGER,
-- 	is_active BOOLEAN,
-- 	PRIMARY KEY (player_name, current_season)
-- )


-- cumulative table design
-- yesterday is the existing cumulative schema (created above)
-- today is the source table which is being read from and assimilated into the cumulative table
-- the 
INSERT INTO players
with yesterday as (
select *
from players where current_season = 2021
), today AS (
select *
from player_seasons where season = 2022
)
SELECT
COALESCE (t.player_name, y.player_name) player_name,
COALESCE (t.height, y.height) height,
COALESCE (t.college, y.college) college,
COALESCE (t.country, y.country) country,
COALESCE (t.draft_year, y.draft_year) draft_year,
COALESCE (t.draft_round, y.draft_round) draft_round,
COALESCE (t.draft_number, y.draft_number) draft_number,
CASE WHEN y.season_stats IS NULL 
 THEN ARRAY[ROW(t.season, t.gp, t.pts, t.reb, t.ast)::season_stats]
 WHEN t.season IS NOT NULL THEN y.season_stats || ARRAY[ROW(t.season, t.gp, t.pts, t.reb, t.ast)::season_stats]
 ELSE y.season_stats
 END AS season_stats,
 CASE WHEN t.season IS NOT NULL 
 THEN 
 	CASE WHEN t.pts > 20 THEN 'star'
	 WHEN t.pts > 15 THEN 'good'
	 WHEN t.pts > 10 THEN 'average'
	 ELSE 'bad'
	END::scoring_class
 ELSE y.scoring_class
 END AS scoring_class,
 
 CASE WHEN t.season IS NOT NULL THEN 0
  ELSE y.years_since_last_season + 1 END AS years_since_last_season,
 COALESCE(t.season, y.current_season + 1) AS current_season,
 CASE WHEN t.season IS NULL THEN 0::Boolean ELSE 1::Boolean END AS is_active
FROM today AS t 
FULL OUTER JOIN yesterday AS y
ON t.player_name = y.player_name;





select player_name, 
(season_stats[CARDINALITY(season_stats)]::season_stats).pts/
CASE WHEN (season_stats[1]::season_stats).pts = 0 THEN 1 
	ELSE (season_stats[1]::season_stats).pts END as latest_season_div_by_current
from players where current_Season = 2001 AND scoring_class = 'star' 
-- NEEDS NO GROUP BY: HAS a MAP but no REDUCE
ORDER BY 2 DESC


WITH unnested AS (
select player_name, (season_stats[1]::season_stats).pts AS first_season,
season_stats[CARDINALITY(season_stats)] as latest_season
from players where current_Season = 2000 AND player_name LIKE '%Michael Jordan%')

SELECT player_name,
	(season_stats::season_stats).*,
	(scoring_class::scoring_class).*
FROM unnested
;




-- ************************************************************************************************************************
-- ********************************************************** Homework ****************************************************
-- ************************************************************************************************************************

select * from actor_films;

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
	2007 AS current_year
FROM streaks
GROUP BY actor, streak_identifier, quality_class, is_active
ORDER BY actor, streak_identifier;


-- ********************************************************** Day 2  *********************************************************


CREATE TABLE players_scd (
	player_name TEXT,
	streak_identifier TEXT, -- tracks how many changes there were to the tracked columns
	-- the next 2 columns are the ones we want to track in the SCD
	is_active BOOLEAN,
	scoring_class scoring_class,	
	start_season INTEGER,
	end_season INTEGER,
	current_season INTEGER,
	PRIMARY KEY (player_name, start_season) --start_season is the other composite key since each value will show a change for the season (making that start_season unique)
)

-- the query below uses Window FUnctions 2 times before substantially reducing the data volume. The Window function parts wre what make it expensive.
-- when there's multiple entries for a single user in the SCD table, it skewes the cardinality of the columns which may potentially slow sown the query over time (but it's still worth it when running it at scale).

 -- Type 2 SCD
-- 	Approach 1

INSERT INTO players_scd
WITH previous AS (
select player_name, 
		current_season,
		scoring_class, 
		is_active,
		LAG(scoring_class, 1) OVER (PARTITION BY player_name ORDER BY current_season) As prev_scoring_class,
		LAG(is_active, 1) OVER (PARTITION BY player_name ORDER BY current_season) As prev_is_active
		
from players
where current_season <= 2021
),

indicators AS (
SELECT *,
CASE WHEN scoring_class <> prev_scoring_class THEN 1 
	 WHEN is_active <> prev_is_active THEN 1 
	ELSE 0 
	END AS change_indicator
FROM PREVIOUS
),

streaks AS (
-- sums how many times the change_indicator changed
SELECT *, SUM(change_indicator) OVER (PARTITION BY player_name ORDER BY current_season) AS streak_identifier
FROM indicators)

-- collapses the rows down to when there was a change
SELECT player_name, streak_identifier, is_active, scoring_class,
		MIN(current_season) AS start_season,
		MAX(current_season) AS end_season,
		2021 AS CURRENT_SEASON
FROM streaks 
GROUP BY player_name, streak_identifier, is_active, scoring_class
ORDER BY player_name, streak_identifier;




-- 	Approach 2

CREATE TYPE scd_type AS (
			scoring_class scoring_class,
			is_active boolean,
			start_season INTEGER,
			end_season INTEGER
)



with last_season AS (
select * from players_scd
where current_season = 2021
and end_season = 2021
),

-- this identifies the records for which change has already been recorded 
-- i.e., they will not change
historical_scd AS (
select
	player_name,
	scoring_class,
	is_active,
	start_season,
	end_season
from players_scd
where current_season = 2021
and end_season < 2021

),

this_season_data AS (
select * from players where current_season = 2022
),
unchanged_records AS (
select tsd.player_name, tsd.scoring_class, tsd.is_active,
						ls.start_season, tsd.current_season AS end_season from this_season_data tsd
-- this season may or may not have data which may or may not close last season's records
-- in case there is new data, a new record will be added. If not, the record will be closed
-- the where condition ensures we only get unchanged records for the columns being tracked
join last_season ls 
ON tsd.player_name = ls.player_name 
WHERE tsd.scoring_class = ls.scoring_class
	AND tsd.is_active = ls.is_active),
	
changed_records AS (
	select tsd.player_name,
	-- the where condition ensures we only get changed records for the columns being tracked
	-- this will yield 2 records: one where the scd continues for this season and one where it stops
	
			UNNEST(ARRAY [
				ROW(ls.scoring_class,
		 			ls.is_active,
		 			ls.start_season,
					ls.end_season)::scd_type,
					
				ROW(tsd.scoring_class,
		 			tsd.is_active,
		 			tsd.current_season,
					tsd.current_season)::scd_type
				
			]) AS records
	from this_season_data tsd		
	left join last_season ls  -- allows for records where last_season may not have data
	ON tsd.player_name = ls.player_name 
	WHERE tsd.scoring_class <> ls.scoring_class
		OR tsd.is_active <> ls.is_active
	),
unnested_changed_records AS (

	select player_name, 
		
		(records::scd_type).scoring_class,
		(records::scd_type).is_active,
		(records::scd_type).start_season,
		(records::scd_type).end_season
	from changed_records

),

new_records AS (
	select tsd.player_name, tsd.scoring_class, tsd.is_active,
							tsd.current_season AS start_season, 
							tsd.current_season AS end_season -- the player started this year						
	from this_season_data tsd
	left join last_season ls 
	ON tsd.player_name = ls.player_name 
	WHERE ls.player_name IS NULL)

select * from historical_scd
UNION ALL 
select * from unchanged_records
UNION ALL
select * from unnested_changed_records
UNION ALL
select * from new_records;

-- ********************************************************** Day 3  *********************************************************
-- ********************************************************** Graph Data Modelling  *********************************************************
-- DROP TABLE Vertices

CREATE TYPE vertex_type
AS ENUM('player', 'team', 'game');

CREATE TABLE Vertices(
identifier TEXT,
type vertex_type,
properties JSON,
PRIMARY KEY(identifier, type)
);



CREATE TYPE edge_type 
AS ENUM ('plays_against', 
		'shares_team_with', 
		'plays_in', 
		'plays_for')


create table Edges(
subject_identifier TEXT,
subject_type vertex_type,
object_identifier TEXT,
object_type vertex_type,
edge_type edge_type,
properties JSON,
PRIMARY KEY(subject_identifier, subject_type, object_identifier, object_type, edge_type)
-- :(
)



-- CREATING THE VERTICES

-- games
INSERT INTO Vertices
-- instantiates the games vertices
select 
	game_id as identifier,
	'game'::vertex_type AS type, 
	json_build_object(
		'pts_home', pts_home,
		'pts_away', pts_away,
		'winning_team', CASE WHEN home_team_wins = 1 THEN home_team_id ELSE visitor_team_id END 
		) as properties
from games


-- players - the properties column has to be aggregated inside a CTE
INSERT INTO Vertices
WITH player_agg AS (
SELECT
	player_id AS identifier,
	MAX(player_name) AS player_name, -- used MAX cuz SQL wants an AGG function here; it doesn't matter which one since they all have the same value anyway
	COUNT(1) AS number_of_games,
	SUM(pts) AS total_points,
	ARRAY_AGG(DISTINCT team_id) AS teams
from game_details
group by player_id )


SELECT
	identifier AS identifier,
	'player'::vertex_type AS type,
	json_build_object( 
		'player_name', player_name,
		'number_of_games', number_of_games,
		'total_points', total_points,
		'teams', teams	
		) AS properties
from player_agg



-- teams
INSERT INTO Vertices
-- the teams table had dupes in it for some reason
WITH teams_deduped AS (
SELECT *, ROW_NUMBER() OVER(PARTITION BY team_id) as rowNum
from teams
)
SELECT 
	team_id AS identifier,
	'team'::vertex_type AS type,
	json_build_object(
		'abbreviation', abbreviation,
		'nickname', city||' '||nickname,
		'city', city,
		'arena', arena,
		'year_founded', yearfounded
		) AS properties
from teams_deduped
where rowNum = 1




-- Populate Edges

-- plays_in
INSERT INTO Edges
WITH deduped_games AS (
SELECT 
	*,	ROW_NUMBER() OVER(PARTITION BY player_id, game_id) AS row_numba
FROM  game_details
)
-- modelling the plays in relationship i.e, a player plays in a game
SELECT
	player_id AS subject_identifier,
	'player'::vertex_type AS subject_type,
	game_id AS object_identifier,
	'game'::vertex_type AS object_type,
	'plays_in'::edge_type AS edge_type,
	json_build_object(
			'start_position', start_position,
			'pts', pts,
			'team_id', team_id,
			'team_abbreviation', team_abbreviation
	) AS properties

from deduped_games
where row_numba = 1




-- shares_team_with and plays_against
INSERT INTO Edges
WITH deduped_games AS (
SELECT 
	*,	ROW_NUMBER() OVER(PARTITION BY player_id, game_id) AS row_numba
FROM  game_details
),

filtered AS(
	SELECT *
	FROM deduped_games 
	WHERE row_numba = 1
),

aggregated AS (
-- doesn't distinguish b/w plays_against and shares_team_with (gets all players who shared a game_id)
SELECT 
	f1.player_id AS subject_player_id, 
	f2.player_id AS object_player_id,
	CASE WHEN (f1.team_abbreviation = f2.team_abbreviation) 
			THEN 'shares_team_with'::edge_type -- play for the same team
		ELSE 'plays_against'::edge_type
	END AS edge_type,
	-- some players changed names so the id is the same with different names
	MAX(f1.player_name) AS subject_player_name,
	MAX(f2.player_name) AS object_player_name,
	COUNT(1) as num_games, 
	SUM(f1.pts) AS subject_points,
	SUM(f2.pts) AS object_points
from filtered f1
	JOIN filtered f2 
	ON f1.game_id = f2.game_id
	and f1.player_name != f2.player_name
WHERE f1.player_id > f2.player_id
-- the WHERE copndition here allows for filtering for only one set of edges b/w 2 vertices
GROUP BY 
	f1.player_id,
	f2.player_id,
	CASE WHEN (f1.team_abbreviation = f2.team_abbreviation) 
			THEN 'shares_team_with'::edge_type -- play for the same team
		ELSE 'plays_against'::edge_type
	END

)

SELECT 
	subject_player_id AS subject_identifier,
	'player'::vertex_type AS subject_type,
	object_player_id AS object_identifier,
	'player'::vertex_type AS object_type,
	edge_type AS edge_type,
	json_build_object(
		'num_games', num_games,
		'subject_points', subject_points,
		'object_points', object_points
	) AS properties
FROM aggregated





-- plays_for edges i.e the team the player played in for a specific game
INSERT INTO Edges
WITH deduped_games AS (
SELECT 
	((SPLIT_PART(min, ':', 1))::numeric * 60) +
	SPLIT_PART(min, ':', 2)::numeric AS total_seconds,
	ROW_NUMBER() OVER(PARTITION BY team_id, player_id ,game_id) AS row_numba,
	*
FROM  game_details
),
 aggregated_games AS (
SELECT
	player_id AS player_id,
	team_id AS team_id,
	SUM(pts) AS total_points,
	SUM(ast) AS total_assists,
	COUNT(*) games_played_for_team,
	SUM(total_seconds/60) total_min_played_for_team,
	STRING_AGG(game_id::text, ',') AS list_of_games
	
	-- properties->>'game_id'
FROM deduped_games dg
WHERE row_numba = 1	
GROUP BY 1, 2
ORDER BY 1, 2
)

SELECT 
	player_id AS subject_identifier,
	'player'::vertex_type AS subject_type,
	team_id AS object_identifier,
	'team'::vertex_type AS object_type,
	'plays_for'::edge_type AS edge_type,
	json_build_object(
		'total_points', total_points,
		'total_assists', total_assists,
		'games_played_for_team', games_played_for_team,
		'total_min_played_for_team', total_min_played_for_team,
		'list_of_games', list_of_games
	) AS properties
FROM aggregated_games






--****************************************** ANALYSIS QUERIES ******************************************

-- plays_in
SELECT 
	v.properties->>'player_name',
	MAX(CAST(e.properties->>'pts'AS INTEGER))
from Vertices AS v
join Edges AS e
on e.subject_identifier = v. identifier
and e.subject_type = v.type
group by 1
order by 2 DESC




-- shares_team_with and plays_against
SELECT 
	v.properties->>'player_name' AS player_name,
	e.properties->>'subject_points' AS own_points,
	CAST(v.properties->>'total_points' as REAL)/
	CASE WHEN CAST(v.properties->>'number_of_games' as REAL) = 0 THEN 1 
		ELSE CAST(v.properties->>'number_of_games' as REAL) END AS career_game_point_avg, -- div by 0 error handled
	e.properties->>'object_points' AS other_players_points,
	e.properties->>'num_games' AS number_of_games
FROM Vertices v
JOIN Edges e on v.identifier = e.subject_identifier
and v.type = e.subject_type
WHERE e.object_type = 'player'::vertex_type




-- stats for each team a player played for
SELECT 
v.properties->>'player_name' AS player_name,
t.city||' '||t.nickname AS team,
CAST(e.properties->>'total_points'AS INTEGER)/CAST(e.properties->>'games_played_for_team'AS INTEGER) AS avg_points_per_game,
ROUND((CAST(e.properties->>'total_points'AS INTEGER)/CAST(e.properties->>'total_min_played_for_team'AS FLOAT))::numeric, 2) AS avg_points_per_min,
from Vertices AS v
join Edges AS e
on e.subject_identifier = v. identifier
and e.subject_type = v.type
and e.edge_type = 'plays_for'
join teams t
on e.object_identifier = (t.team_id)::text






