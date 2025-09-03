

create  table device_hits_dashboard as
with events_augmented AS (

select COALESCE(d.os_type, 'Unknown') as os_type,
COALESCE(d.device_type, 'Unknown') as device_type,
COALESCE(d.browser_type, 'Unknown') as browser_type,
url,
user_id
from events e
join devices d on e.device_id = d.device_id

)

select 
	-- GROUPING(os_type) as os_type_not_grouped,
	-- GROUPING(device_type) as device_type_not_grouped,
	-- GROUPING(browser_type) as browser_type_not_grouped,
	CASE 
		WHEN 
			GROUPING(os_type) = 0 AND
			GROUPING(device_type) = 0 AND
			GROUPING(browser_type) = 0 THEN 'os_type___device_type___browser_type'
		WHEN GROUPING(os_type) = 0 THEN 'os_type'
		WHEN GROUPING(device_type) = 0 THEN 'device_type'
		WHEN GROUPING(browser_type) = 0 THEN 'browser_type'
	END AS aggregation_level,
			
	COALESCE(os_type, '(overall)') as os_type,
	COALESCE(device_type, '(overall)') as device_type,
	COALESCE(browser_type, '(overall)') as browser_type,
	count(*) as number_of_hits
from events_augmented
-- group by cube (os_type, device_type,	browser_type)
-- group by rollup (os_type, device_type,	browser_type)
group by grouping sets(
	(os_type, device_type,	browser_type),
	(browser_type),
	(os_type),
	(device_type)
)
order by count(*) DESC;



-- selecting the aggregation grain with all 3 columns
select * 
from device_hits_dashboard
where aggregation_level = 'os_type___device_type___browser_type';
