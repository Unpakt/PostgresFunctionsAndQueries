SELECT * FROM move_plans left join (SELECT distinct move_plan_id FROM jobs ) as unique_jobs ON unique_jobs.move_plan_id = move_plans.id



SELECT count(*), discount_requested, discount_completed, booked FROM (
SELECT
	compare.move_plan_id,
	compare.number_of_movers,
	compare.discount_requested,
	compare.average_price,
	compare.lowest_price,
	compare.average_discount,
	CASE
		WHEN compare.average_discount IS NOT NULL THEN TRUE
		ELSE FALSE
	END as discount_completed,
	CASE
		WHEN booking_indicator IS NOT NULL THEN TRUE
		ELSE FALSE
	END as booked
 FROM (
SELECT move_plan_id,
	count(*) AS number_of_movers,
	bool_or(discount_requested) AS discount_requested,
	round(avg(last_estimate_price)::numeric, 2) AS average_price,
	min(last_estimate_price) AS lowest_price,
	round(avg(discount_amount)::numeric, 2) AS average_discount
FROM estimate_logs where estimate_active GROUP BY estimate_logs.move_plan_id ) as compare
left join (SELECT distinct move_plan_id AS booking_indicator FROM jobs ) as unique_jobs ON unique_jobs.booking_indicator = compare.move_plan_id)
AS RAD GROUP BY discount_requested,discount_completed, booked ORDER BY booked,discount_requested,discount_completed;

SELECT * FROM estimate_logs limit 1;

SELECT count(distinct(move_plan_id)) FROM estimate_logs where discount_amount IS NOT NULL;
SELECT count(distinct(move_plan_id)) FROM estimate_logs where discount_requested = TRUE


SELECT * FROM inventory_items WHERE is_user_generated = TRUE ORDER BY name;