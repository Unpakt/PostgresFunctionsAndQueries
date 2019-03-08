SELECT * FROm move_plans where read_only_plan = true;


SELECT * FROM estimate_logs group by move_plan_id ;

SELECT
	origin_state,
	interstate,
	round(avg(number_on_compare),2) as num_movers,
	count(*) as num_plans,100.00*round((sum(booked)*1.0)/(1.0*count(*)),4) as conversion_rate,
	round(cast(avg(avg_cost) as numeric),2) as avg_cost,
	sum("1-3") as "1-3",
	CASE WHEN sum("1-3") > 0 THEN	round(100.00*((sum("1-3 booked")*1.0)/(sum("1-3")*1.0)),2) ELSE 0.00 END as "1-3 Conversion Rate",
	sum("3-6") as "3-6",
	CASE WHEN sum("3-6") > 0 THEN	round(100.00*((sum("3-6 booked")*1.0)/(sum("3-6")*1.0)),2) ELSE 0.00 END as "3-6 Conversion Rate",
	sum("6-9") as "6-9",
	CASE WHEN sum("6-9") > 0 THEN	round(100.00*((sum("6-9 booked")*1.0)/(sum("6-9")*1.0)),2) ELSE 0.00 END as "6-9 Conversion Rate",
	sum("9-12") as "9-12",
	CASE WHEN sum("9-12") > 0 THEN	round(100.00*((sum("9-12 booked")*1.0)/(sum("9-12")*1.0)),2) ELSE 0.00 END as "9-12 Conversion Rate",
	sum("12-15") as "12-15",
	CASE WHEN sum("12-15") > 0 THEN	round(100.00*((sum("12-15 booked")*1.0)/(sum("12-15")*1.0)),2) ELSE 0.00 END as "12-15 Conversion Rate",
	sum("15+") as "15+",
	CASE WHEN sum("15+") > 0 THEN	round(100.00*((sum("15+ booked")*1.0)/(sum("15+")*1.0)),2) ELSE 0.00 END as "15+ Conversion Rate"
FROM
	(SELECT
		pup.origin_state,
		dof.dof_state != pup.origin_state AS interstate,
		CASE WHEN jobs.move_plan_id IS NOT NULL THEN 1 ELSE 0 END as booked,
		CASE WHEN number_on_compare <= 3 AND jobs.move_plan_id IS NOT NULL THEN 1 ELSE 0 END as "1-3 booked",
		CASE WHEN number_on_compare <= 3 THEN 1 ELSE 0 END as "1-3",
		CASE WHEN number_on_compare > 3 AND number_on_compare <= 6 AND jobs.move_plan_id IS NOT NULL THEN 1 ELSE 0 END as "3-6 booked",
		CASE WHEN number_on_compare > 3 AND number_on_compare <= 6 THEN 1 ELSE 0 END as "3-6",
		CASE WHEN number_on_compare > 6 AND number_on_compare <= 9 AND jobs.move_plan_id IS NOT NULL THEN 1 ELSE 0 END as "6-9 booked",
		CASE WHEN number_on_compare > 6 AND number_on_compare <= 9 THEN 1 ELSE 0 END as "6-9",
		CASE WHEN number_on_compare > 9 AND number_on_compare <= 12 AND jobs.move_plan_id IS NOT NULL THEN 1 ELSE 0 END as "9-12 booked",
		CASE WHEN number_on_compare > 9 AND number_on_compare <= 12 THEN 1 ELSE 0 END as "9-12",
		CASE WHEN number_on_compare > 12 AND number_on_compare <= 15 AND jobs.move_plan_id IS NOT NULL THEN 1 ELSE 0 END as "12-15 booked",
		CASE WHEN number_on_compare > 12 AND number_on_compare <= 15 THEN 1 ELSE 0 END as "12-15",
		CASE WHEN number_on_compare > 15 AND jobs.move_plan_id IS NOT NULL THEN 1 ELSE 0 END as "15+ booked",
		CASE WHEN number_on_compare > 15 THEN 1 ELSE 0 END as "15+",
		compare.*
	FROM move_plans
	JOIN (SELECT state origin_state, move_plan_id FROM addresses where role_in_plan = 'pick_up') AS pup
		ON pup.move_plan_id = move_plans.id
	JOIN (SELECT state dof_state, move_plan_id FROM addresses where role_in_plan = 'drop_off') as dof
		ON dof.move_plan_id = move_plans.id
	JOIN (SELECT count(*) as number_on_compare, move_plan_id, avg(last_estimate_price) avg_cost FROM estimate_logs GROUP BY move_plan_id) AS compare
		ON compare.move_plan_id = move_plans.id
	LEFT JOIN (SELECT distinct move_plan_id from jobs) as jobs ON jobs.move_plan_id = move_plans.id)
AS all_plans GROUP BY origin_state, interstate;