DROP FUNCTION IF EXISTS mover_life_cycle(DATE, DATE);
CREATE OR REPLACE FUNCTION mover_life_cycle(start_date DATE, end_date DATE)
RETURNS TABLE(
  "Status" TEXT,
  "# of plans difference" BIGINT,
  "Change in Value" NUMERIC,
  "Change in Amount Due to movers" NUMERIC,
  "# of plans at start date" BIGINT,
  "# of plans at end date" BIGINT,
  "Value at start date" NUMERIC,
	"Value at end date" NUMERIC,
	"Mover Cut at start Date" NUMERIC,
	"Mover Cut at end Date" NUMERIC
	) AS
$func$
BEGIN
RETURN QUERY
(SELECT
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
	sum(total*(1-.01*estimate_commission_rate))  mover_cut,
	max(move_date) most_recent_move_date,
	min(move_date) oldest_move_date,
	CASE
		WHEN (SELECT count(*) from payment_entries
		  WHERE chargeable_id = move_plans.id
			AND chargeable_type = 'MovePlan'
			AND updated_at::DATE <= start_date AND updated_at > created_at
			AND payment_id is not null GROUP BY chargeable_id)  > 0
	  THEN '5-paid'
		WHEN move_date::DATE  <= start_date  THEN '4-completed'
		WHEN move_date::DATE  <= (start_date + 2) THEN '3-charged'
		WHEN jobs.created_at::DATE  <= start_date THEN '2-pending_at_date'
		WHEN jobs.created_at::DATE  > start_date THEN '1-future_pending'
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
	sum(total*(1-.01*estimate_commission_rate)) mover_cut,
	max(move_date) most_recent_move_date,
	min(move_date) oldest_move_date,
	CASE
		WHEN (SELECT count(*) from payment_entries
			WHERE chargeable_id = move_plans.id
			AND chargeable_type = 'MovePlan'
			AND updated_at::DATE <= end_date AND updated_at > created_at
			AND payment_id is not null GROUP BY chargeable_id) > 0
	  THEN '5-paid'
		WHEN move_date::DATE  <= end_date THEN '4-completed'
		WHEN move_date::DATE  <= (end_date + 2) THEN '3-charged'
		WHEN jobs.created_at::DATE  <= end_date THEN '2-pending_at_date'
		WHEN jobs.created_at::DATE  > end_date THEN '1-future_pending'
	END as "Status"
	FROM jobs
	JOIN move_plans on move_plans.id = jobs.move_plan_id  And mover_state not in ('declined','cancelled_pending','cancelled_acknowledged','new') AND user_state not in ( 'reserved_cancelled','reserved','cancelled')
	JOIN estimates on estimates.move_plan_id = move_plans.id
	JOIN uuids on uuids.uuidable_id = move_plans.id AND uuids.uuidable_type = 'MovePlan'
	GROUP BY "Status") as second on first."Status" = second."Status");
	END
$func$ LANGUAGE plpgsql;

SELECT * FROM mover_life_cycle('2019-03-11'::DATE,'2019-03-25'::DATE);

SELECT(5344393.9 - 2760272.56);


SELECT distinct estimate_commission_rate FROM move_plans;


SELECT * FROM payment_entries WHERE payment_id IS NULL;