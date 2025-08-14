CREATE TABLE users_growth_accounting(
	user_id TEXT,
	first_active_date DATE,
	last_active_date DATE,
	daily_active_state TEXT,
	weekly_active_state TEXT,
	dates_active DATE[],
	date DATE,
	PRIMARY KEY (user_id, date)

);



-- loads data incrementally
insert into users_growth_accounting
with yesterday as(

	SELECT *
	FROM users_growth_accounting
	WHERE date = '2023-01-09'::date
),

today as (
	
	select
		user_id::text,
		DATE_TRUNC('day', event_time::timestamp) as today_date,
		COUNT(1)
	from events
	where DATE_TRUNC('day', event_time::timestamp) = '2023-01-10'
		and user_id IS NOT NULL
	-- the date you use here is inception, essentially
	group by user_id, DATE_TRUNC('day', event_time::timestamp)
	
)


-- SEED Query
-- The quintessential incremental load query
select 
	COALESCE(t.user_id, y.user_id) as user_id,
	COALESCE(y.first_active_date, t.today_date) as first_active_date, -- if not active yesterday, they're active for the first time today,
	COALESCE(t.today_date, y.last_active_date) as last_active_date,
	
	CASE WHEN y.user_id IS NULL THEN 'New'
		 WHEN y.last_active_date = t.today_date - Interval '1 day' THEN 'Retained'
		 WHEN y.last_active_date < t.today_date - Interval '1 day' THEN 'Ressurected'
		 WHEN t.today_date IS NULL and y.last_active_date = y.date THEN 'Churned'
		 ELSE 'Stale'
	END as daily_active_state,
	
	CASE WHEN y.user_id IS NULL THEN 'New' 
		 WHEN y.last_active_date >= y.date - Interval '7 day' THEN 'Retained'
		 WHEN y.last_active_date < t.today_date - Interval '7 day' THEN 'Ressurected'
		
		 WHEN t.today_date IS NULL AND y.last_active_date = y.date -  Interval '7 day' THEN 'Churned'
		 ELSE 'Stale'
	END as weekly_active_state,
	
	COALESCE(y.dates_active, ARRAY[]::DATE[]) || 
	CASE WHEN t.user_id IS NOT NULL THEN ARRAY[t.today_date]
		ELSE ARRAY[]::DATE[] 
			END AS dates_active,
		COALESCE(t.today_date, y.date + Interval '1 day') as date
from today t
FULL OUTER JOIN yesterday y 
ON t.user_id::text = y.user_id::text;




-- check which day of the week had the greatest retention
select 
	date,
	extract(dow from first_active_date),
	(date - first_active_date) as days_since_signup,
	count(case when daily_active_state IN ('Retained', 'New', 'Ressurected') then 1 end)::real as active_count,
	round( (count(case when daily_active_state IN ('Retained', 'New', 'Ressurected') then 1 end)::real*100
	/count(1))::numeric, 2) as pct_active
from users_growth_accounting
-- where first_active_date = '2023-01-04'
group by date,
	extract(dow from first_active_date),
	(date - first_active_date)
order by date, extract(dow from first_active_date);
