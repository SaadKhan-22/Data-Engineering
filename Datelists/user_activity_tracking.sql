create table user_devices_cumulated (
user_id TEXT,
browser_type TEXT,
active_dt DATE,
dates_active DATE[]

);




insert into user_devices_cumulated
with yesterday as (

select *
from user_devices_cumulated
where active_dt = ('2023-01-30')::date

),

today as (

select 
	e.user_id,
	REPLACE(d.browser_type, ') Bot', 'Bot') as browser_type,
	e.event_time::date as active_date
from events e
left join devices d
	on d.device_id = e.device_id
WHERE e.event_time::date = '2023-01-31'::date
	and e.user_id IS NOT NULL
	and e.event_time IS NOT NULL
GROUP BY 1, 2, 3
ORDER BY 1
)

select
	COALESCE(y.user_id, t.user_id::text) as user_id,
	COALESCE(y.browser_type, t.browser_type) as browser_type,
	COALESCE(t.active_date::date, y.active_dt::date + interval '1 day')::date as active_dt,
	CASE 
		WHEN y.dates_active IS NULL -- if no previous data, use today's to start
			THEN ARRAY[t.active_date]
		WHEN t.active_date IS NULL -- if not active today, propagat previous data forward
			THEN y.dates_active
		ELSE ARRAY[t.active_date] || y.dates_active 
		END as dates_active
		-- if both present then append yesterday to today's end
		-- this dictates the order of dates in the future as being appended to the right
	
from yesterday y
full outer join today t
	on y.user_id::text = t.user_id::text;












CREATE TABLE hosts_cumulated(

user_id TEXT,
browser_Type TEXT,
host_activity_datelist BIT(32),
dim_is_active_L30 TEXT,
dim_is_weekly_active TEXT,
dim_is_yesterday_active TEXT

);

INSERT INTO hosts_cumulated
WITH devices_cumulated as (

SELECT *
FROM user_devices_cumulated 
WHERE active_dt > '2023-01-25'
-- this is the 'current' date for this run. Running at scale will requrie this to be dynamic.
),

date_series as(
SELECT *
FROM generate_series(('2023-01-01')::date, ('2023-01-31')::date, interval '1 day') as date_in_series
),

placeholder_ints as (
select *,
active_dt - (date_in_series::date) as days_since_current_dt,
dates_active @> ARRAY[(date_in_series::date)] as conatins_date_series,
POWER(2, 32 - (active_dt - (date_in_series::date)))::bigint as reversed_int,
-- 2 raised to the power (32 minus days_since_current_date)
	-- the higher the value, the more recent the active status
	CASE WHEN dates_active @> ARRAY[(date_in_series::date)]
		THEN (POWER(2, 32 - (active_dt - (date_in_series::date)))::bigint)
		ELSE (0::bigint) END as placeholder_int_value
	-- The subtraction here only serves to yield a number as a power of 2
from devices_cumulated dc
cross join date_series ds
)



select 
	user_id,
	browser_type,
	-- sum(placeholder_int_value), --summed for this user
	(sum(placeholder_int_value)::bigint)::bit(32) as host_activity_datelist, --active_days_L30
	BIT_COUNT((sum(placeholder_int_value)::bigint)::bit(32)) > 0 as dim_is_active_L30, -- at least one bit is ON
	
	BIT_COUNT(CAST('11111110000000000000000000000000' AS bit(32)) & 
    -- this line above is a mask which has the last 7 days as active
	(sum(placeholder_int_value)::bigint)::bit(32) ) > 0 AS dim_is_weekly_active,
	
	BIT_COUNT(CAST('1000000000000000000000000000000' AS bit(32)) &
    -- this line above is a mask which has yesterday as active
	(sum(placeholder_int_value)::bigint)::bit(32) ) > 0 AS dim_is_yesterday_active
from placeholder_ints
GROUP by 1, 2
ORDER by 1;

