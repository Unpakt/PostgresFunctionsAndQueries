SELECT
	clicks.*,
	total as total_plans,
	ROUND(CAST(email+phone+website AS numeric)*100.00/CAST(total AS numeric),2) AS clicks_per_plan,
	conversion
FROM (
	SELECT
		extract(MONTH FROM created_at) as month,
		extract(YEAR FROM created_at) as year,
		count(CASE WHEN lead_type = 'email_click' THEN 1 END) as Email,
		count(CASE WHEN lead_type = 'phone_click' THEN 1 END) as Phone,
		count(CASE WHEN lead_type = 'url_click' THEN 1 END) as Website
	FROM leads
	GROUP BY
		extract(MONTH FROM created_at),
		extract(YEAR FROM created_at)
	ORDER BY extract(YEAR FROM created_at),extract(MONTH FROM created_at)
	) AS clicks
	JOIN
	(SELECT
		extract(MONTH FROM mp.created_at) as month,
		extract(YEAR FROM mp.created_at) as year,
		ROUND(CAST(count(jb.id) AS NUMERIC)/CAST(count(*) AS NUMERIC) * 100,2) AS conversion,
		count(*) AS total
	FROM move_plans AS mp
	LEFT JOIN jobs AS jb on jb.move_plan_id = mp.id
	GROUP BY
		extract(YEAR FROM mp.created_at),
		extract(MONTH FROM mp.created_at)
	ORDER BY extract(YEAR FROM mp.created_at),extract(MONTH FROM mp.created_at)) as conversions
	ON conversions.month = clicks.month AND conversions.year = clicks.year
