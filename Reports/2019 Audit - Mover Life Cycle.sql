SELECT
	coalesce(first."Status",second."Status") as "Status",+
	coalesce(second.num_plans,0) - coalesce(first.num_plans,0) as "# of plans difference",
	coalesce(second.total,0.00) - coalesce(first.total,0.00) as "Change in Value",
	coalesce(second.mover_cut,0.00) - coalesce(first.mover_cut, 0.00) as "Change in Amount Due to movers",
	first.num_plans as "# of plans at start date",
	second.num_plans as "# of plans at end date",
	first.total as "Value at start date",
	second.total as "Value at end date",
	first.mover_cut as "Mover Cut at start Date",
	second.mover_cut as "Mover Cut at end Date"
	FROM
	(SELECT
	count(*) num_plans,
	round(sum(total),2) total,
	sum(mover_cut) mover_cut,
	max(move_date) most_recent_move_date,
	min(move_date) oldest_move_date,
	CASE WHEN (SELECT count(*) from payment_entries WHERE chargeable_id = move_plans.id AND chargeable_type = 'MovePlan' AND updated_at::DATE <= '2019-03-11' AND updated_at > created_at AND payment_id is not null GROUP BY chargeable_id)  > 0 THEN '5-paid'
		WHEN (SELECT count(*) from payment_entries WHERE chargeable_id = move_plans.id AND chargeable_type = 'MovePlan' AND created_at::DATE <= '2019-03-11' AND payment_id is null GROUP BY chargeable_id) > 0 THEN '4b-verified'
		WHEN move_date::DATE  <= ('2019-03-11'::DATE ) THEN '4a-completed'
		WHEN move_date::DATE  <= ('2019-03-11'::DATE + 2) THEN '3-charged'
		WHEN jobs.created_at::DATE  <= ('2019-03-11'::DATE ) THEN '2-pending_at_date'
		WHEN jobs.created_at::DATE  > ('2019-03-11'::DATE ) THEN '1-future_pending'
	END as "Status"
	FROM jobs
	JOIN move_plans on move_plans.id = jobs.move_plan_id And mover_state not in ('declined','cancelled_pending','cancelled_acknowledged','new') AND user_state not in ( 'reserved_cancelled','reserved','cancelled')
	JOIN estimates on estimates.move_plan_id = move_plans.id
	JOIN uuids on uuids.uuidable_id = move_plans.id AND uuids.uuidable_type = 'MovePlan'
	GROUP BY "Status") as first
 LEFT JOIN
  (SELECT
	count(*) num_plans,
	round(sum(total),2) total,
	sum(mover_cut) mover_cut,
	max(move_date) most_recent_move_date,
	min(move_date) oldest_move_date,
	CASE WHEN (SELECT count(*) from payment_entries WHERE chargeable_id = move_plans.id AND chargeable_type = 'MovePlan' AND updated_at::DATE <= '2019-03-25' AND updated_at > created_at AND payment_id is not null GROUP BY chargeable_id) > 0 THEN '5-paid'
		WHEN (SELECT count(*) from payment_entries WHERE chargeable_id = move_plans.id AND chargeable_type = 'MovePlan' AND created_at::DATE <= '2019-03-25' AND payment_id is null GROUP BY chargeable_id) > 0 THEN '4b-verified'
		WHEN move_date::DATE  <= ('2019-03-25'::DATE ) THEN '4a-completed'
		WHEN move_date::DATE  <= ('2019-03-25'::DATE + 2) THEN '3-charged'
		WHEN jobs.created_at::DATE  <= ('2019-03-25'::DATE ) THEN '2-pending_at_date'
		WHEN jobs.created_at::DATE  > ('2019-03-25'::DATE ) THEN '1-future_pending'
	END as "Status"
	FROM jobs
	JOIN move_plans on move_plans.id = jobs.move_plan_id  And mover_state not in ('declined','cancelled_pending','cancelled_acknowledged','new') AND user_state not in ( 'reserved_cancelled','reserved','cancelled')
	JOIN estimates on estimates.move_plan_id = move_plans.id
	JOIN uuids on uuids.uuidable_id = move_plans.id AND uuids.uuidable_type = 'MovePlan'
	GROUP BY "Status") as second on first."Status" = second."Status";


SELECT distinct user_state FROM jobs;