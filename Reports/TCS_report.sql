SELECT "delayed_jobs".* FROM "delayed_jobs";

SELECT * FROM move_plans where id = 711566;

SELECT
trav_id AS        "Travel ID",
empl_id AS        "Employee ID",
cust_name AS      "Customer Name",
mpid AS           "MPID",
pu_street AS      "PU Street",
pu_city AS        "PU City",
pu_state AS       "PU State",
pu_zip AS         "PU Zip",
do_street AS      "DO Street",
do_city AS        "DO City",
do_state AS       "DO State",
do_zip AS         "DO Zip",
ii_count AS       "# Inventory Items",
distance AS       "Miles",
cf AS             "Cubic Feet",
wt AS             "Weight",
started_date AS   "MP Started",
move_date AS      "Move Date",
days_notice AS    "Days Notice",
budget AS         "Budget",
price_array[1] AS "# of Movers",
price_array[2] AS "Lowest Price",
mover_selected AS "Mover Selected",
booked_price AS   "Booked Price",
amount_above AS   "Amount Above Budget",
booking_date AS   "Booking Date",
booked_mover AS   "Booked Mover",
job_status AS     "Job Status"
FROM
(SELECT
mp.contact_last_name AS                                      trav_id,
mp.contact_first_name AS                                     empl_id,
pro.name AS                                                  cust_name,
mp.id AS                                                     mpid,
p_u.street_address AS                                        pu_street,
p_u.city AS                                                  pu_city,
p_u.state AS                                                 pu_state,
p_u.zip AS                                                   pu_zip,
d_o.street_address AS                                        do_street,
d_o.city AS                                                  do_city,
d_o.state AS                                                 do_state,
d_o.zip AS                                                   do_zip,
items.num_items AS                                           ii_count,
mp.distance_in_miles AS                                      distance,
items.cf AS                                                  cf,
items.cf * 7 AS                                              wt,
mp.created_at AS                                             started_date,
mp.move_date AS                                              move_date,
(mp.move_date::Date - mp.created_at::Date) AS                days_notice,
NULL AS                                                      delivery_date,
mp.source_budget AS                                          budget,
(SELECT * FROM safe_movers_count_and_min_price(uid.uuid)) AS price_array,
CASE WHEN mpm.mover_id IS NOT NULL THEN
	TRUE
ELSE
	FALSE
END AS                                                      mover_selected,
bid.total AS                                                booked_price,
CASE WHEN (bid.total - mp.source_budget) > 0 THEN
	(bid.total - mp.source_budget)
ELSE
	0.00
END AS                                                    amount_above,
jb.created_at AS                                          booking_date,
bp.name AS                                                booked_mover,
jb.user_state AS                                          job_status
FROM move_plans AS mp
JOIN addresses AS p_u
	ON p_u.move_plan_id = mp.id
	AND p_u.role_in_plan = 'pick_up'
	AND mp.source = 'tcs'
LEFT JOIN addresses AS d_o
	ON d_o.move_plan_id = mp.id
	AND d_o.role_in_plan = 'drop_off'
LEFT JOIN jobs AS jb
	ON jb.move_plan_id = mp.id
LEFT JOIN price_charts AS pc
	ON pc.id = jb.price_chart_id
LEFT JOIN branch_properties AS bp
	ON bp.branchable_id = pc.mover_id AND branchable_type = 'Mover'
JOIN (SELECT count(*) as num_items,
			sum(cubic_feet) AS cf,
			move_plan_id
			FROM move_plan_inventory_items AS mpii
			JOIN inventory_items AS ii
				ON ii.id = mpii.inventory_item_id
			GROUP BY move_plan_id) AS items
	ON items.move_plan_id = mp.id
JOIN uuids AS uid
ON uid.uuidable_id = mp.id
AND uid.uuidable_type = 'MovePlan'
LEFT JOIN ownerships AS own ON own.move_plan_id = jb.move_plan_id
LEFT JOIN users AS u ON u.id = own.user_id
LEFT JOIN profiles AS pro ON pro.user_id = u.id
LEFT JOIN bids AS bid ON bid.move_plan_id = mp.id
LEFT JOIN move_plan_movers AS mpm ON mpm.move_plan_id = mp.id) as thing;



SELECT user_state, count(*) FROM jobs where move_plan_id in (SELECT id from move_plans where move_plans.white_label_id = 1) GROUP BY user_state;


SELECT * FROM uuids where uuid ilike 'B4DE61%';

SELECT max(created_at) FROM move_plans;

SELECT user_state, count(*) FROM jobs GROUP BY user_state;
SELECT mover_state, count(*) FROM jobs GROUP BY mover_state;


SELECT * FROM move_plans where white_label_id = 1;

SELECT * FROM '{45.2, 23.2}'