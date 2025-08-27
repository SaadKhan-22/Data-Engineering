olympics

player_id	year_won	medal_type


player ids which won gold in 3 consec years

222 2000 2003 2006
222 2000 2001

with gold_years as(
select player_id, year_won,
    LAG(year_won, 1) OVER (PARTITION BY player_id, ORDER BY year_won) second_to_last_year_for_gold,
    LAG(year_won, 2) OVER (PARTITION BY player_id, ORDER BY year_won) last_year_for_gold,
from olympics
where medal_type = 'Gold'
order by year_won )


select *
from gold_years
where last_year_for_gold + interval 2 year = year_won;










