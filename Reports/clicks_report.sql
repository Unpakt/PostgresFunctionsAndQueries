SELECT
	clicks.*,
	total as total_plans,
	ROUND(CAST(email+phone+website AS numeric)*100.00/CAST(total AS numeric),2) AS clicks_per_plan,
	conversion
FROM (
	SELECT
		date_trunc('day', created_at) as date,
		count(CASE WHEN lead_type = 'email_click' THEN 1 END) as Email,
		count(CASE WHEN lead_type = 'phone_click' THEN 1 END) as Phone,
		count(CASE WHEN lead_type = 'url_click' THEN 1 END) as Website
	FROM leads
	GROUP BY
	date_trunc('day', created_at)
	ORDER BY date_trunc('day', created_at)
	) AS clicks
	JOIN
	(SELECT
		date_trunc('day', mp.created_at) as date,
		ROUND(CAST(count(jb.id) AS NUMERIC)/CAST(count(*) AS NUMERIC) * 100,2) AS conversion,
		count(*) AS total
	FROM move_plans AS mp
	LEFT JOIN jobs AS jb on jb.move_plan_id = mp.id
	GROUP BY
		date_trunc('day', mp.created_at)
	ORDER BY date_trunc('day', mp.created_at)) as conversions
	ON clicks.date =  conversions.date
