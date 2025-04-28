
-- Creating  a Vertices table with custom type
CREATE TYPE vertex_type
AS ENUM('player', 'team', 'game');

CREATE TABLE Vertices(
identifier TEXT,
type vertex_type,
properties JSON,
PRIMARY KEY(identifier, type)
);


-- Creating  an Edges table with custom type
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



-- CREATING THE VERTICES (all the ones in the Enum at ln 4)

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




-- Populate Edges after all Vertices are done

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

-- Max points a player had
SELECT 
	v.properties->>'player_name',
	MAX(CAST(e.properties->>'pts'AS INTEGER))
from Vertices AS v
join Edges AS e
on e.subject_identifier = v. identifier
and e.subject_type = v.type
group by 1
order by 2 DESC




-- Career game point average compared to other players polayed with/against
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



