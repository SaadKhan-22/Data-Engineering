
- Create a cumulative users table grouped by `users, browser_type` which lists the dates the user was active on before the current date. For example, if a user is active on the 1st, 2nd, and 4th of a month, only the record for the 4th will show all 3 dates:

![[Pasted image 20250618054437.png]]

- CROSS JOIN this with a `generate_series` CTE which generates dates for 30 days. We'll only really be interested in the latest date (usually). This also depends on the historical data available for the user.
1.  Create an `active_dt - (date_in_series::date) as days_since_current_dt` column. This will give the days since current date.
2.  If the `dates_active` array for the date we're interested in contains a date(s) from the `generate_series` CTE above (revisit PGSQL array comparisons) for all the records of the CTE, we do the following: 
	1. Perform 2^(32 - `days_since_current_dt`) or **n** in place of 32 where n is the number of days of history we required
	2. This gives a reversed number i.e., `POWER(2, 32 - days_since_current_dt) ` which will be <= `2^32 `. Since we're considering the latest date for a one-month period, this will give a large number. As we go further in history, the value for the expression will decrease and the result will always be a power of 2 (either 2, 4, or 8). This is `placeholder_int_value` in the code.
	3. We then add all these power-of-2 numbers to get one number, grouped by `user_id`. This is cast to binary. This will give us the number needed: smaller powers of 2 indicate older days, larger ones indicate more recent days, with the MSB indicating yesterday. 1s will indicate that the user was active on those days, 0s will indicate inactivity. Using binary masks can give us many important flags and dimensions e.g., was the user active once during the entire month, throughout last week, yesterday etc.
3. To alternate the order of days based on recency, do not subtract from 32. Instead, use only the `days_since_current_dt` value as the power of 2. This will make the recent days greater powers of 2.