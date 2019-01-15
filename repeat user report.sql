SELECT
sum(first_2012) as first_2012, sum(second_2012) as second_2012, sum(third_2012) as third_2012, sum(fourth_2012) as fourth_2012, sum(fifth_2012) as fifth_2012, sum(sixth_2012) as sixth_2012, sum(seventh_2012) as seventh_2012,
sum(first_2013) as first_2013, sum(second_2013) as second_2013, sum(third_2013) as third_2013, sum(fourth_2013) as fourth_2013, sum(fifth_2013) as fifth_2013, sum(sixth_2013) as sixth_2013, sum(seventh_2013) as seventh_2013,
sum(first_2014) as first_2014, sum(second_2014) as second_2014, sum(third_2014) as third_2014, sum(fourth_2014) as fourth_2014, sum(fifth_2014) as fifth_2014, sum(sixth_2014) as sixth_2014, sum(seventh_2014) as seventh_2014,
sum(first_2015) as first_2015, sum(second_2015) as second_2015, sum(third_2015) as third_2015, sum(fourth_2015) as fourth_2015, sum(fifth_2015) as fifth_2015, sum(sixth_2015) as sixth_2015, sum(seventh_2015) as seventh_2015,
sum(first_2016) as first_2016, sum(second_2016) as second_2016, sum(third_2016) as third_2016, sum(fourth_2016) as fourth_2016, sum(fifth_2016) as fifth_2016, sum(sixth_2016) as sixth_2016, sum(seventh_2016) as seventh_2016,
sum(first_2017) as first_2017, sum(second_2017) as second_2017, sum(third_2017) as third_2017, sum(fourth_2017) as fourth_2017, sum(fifth_2017) as fifth_2017, sum(sixth_2017) as sixth_2017, sum(seventh_2017) as seventh_2017,
sum(first_2018) as first_2018, sum(second_2018) as second_2018, sum(third_2018) as third_2018, sum(fourth_2018) as fourth_2018, sum(fifth_2018) as fifth_2018, sum(sixth_2018) as sixth_2018, sum(seventh_2018) as seventh_2018
FROM (
SELECT 1 as coal,
id,
case when count2012 > 0 then 1 else 0 end as first_2012,
case when count2013 > 0 AND (count2012) = 0 then 1 else 0 end as first_2013,
case when count2014 > 0 AND (count2012 + count2013) = 0 then 1 else 0 end as first_2014,
case when count2015 > 0 AND (count2012 + count2013 + count2014) = 0 then 1 else 0 end as first_2015,
case when count2016 > 0 AND (count2012 + count2013 + count2014 + count2015) = 0 then 1 else 0 end as first_2016,
case when count2017 > 0 AND (count2012 + count2013 + count2014 + count2015 + count2016) = 0 then 1 else 0 end as first_2017,
case when count2018 > 0 AND (count2012 + count2013 + count2014 + count2015 + count2016 + count2017) = 0 then 1 else 0 end as first_2018,
case when count2012 > 1 then 1 else 0 end as second_2012,
case when (count2013 > 0 AND (count2012 + count2013) = 2) or (((count2012) < 2) and ((count2012 + count2013) > 2)) then 1 else 0 end as second_2013,
case when (count2014 > 0 AND (count2012 + count2013 + count2014) = 2) or ((count2012 + count2013) < 2 and (count2012 + count2013 + count2014) > 2) then 1 else 0 end as second_2014,
case when (count2015 > 0 AND (count2012 + count2013 + count2014 + count2015) = 2) or ((count2012 + count2013 + count2014) < 2 and (count2012 + count2013 + count2014 + count2015) > 2) then 1 else 0 end as second_2015,
case when (count2016 > 0 AND (count2012 + count2013 + count2014 + count2015 + count2016) = 2) or ((count2012 + count2013 + count2014 + count2015) < 2 and (count2012 + count2013 + count2014 + count2015 + count2016) > 2) then 1 else 0 end as second_2016,
case when (count2017 > 0 AND (count2012 + count2013 + count2014 + count2015 + count2016 + count2017) = 2) or ((count2012 + count2013 + count2014 + count2015 + count2016) < 2 and (count2012 + count2013 + count2014 + count2015 + count2016 + count2017) > 2) then 1 else 0 end as second_2017,
case when (count2018 > 0 AND (count2012 + count2013 + count2014 + count2015 + count2016 + count2017 + count2018) = 2) or ((count2012 + count2013 + count2014 + count2015 + count2016 + count2017) < 2 and (count2012 + count2013 + count2014 + count2015 + count2016 + count2017 + count2018) > 2) then 1 else 0 end as second_2018,
case when count2012 > 2 then 1 else 0 end as third_2012,
case when (count2013 > 0 AND (count2012 + count2013) = 3) or (((count2012) < 3) and ((count2012 + count2013) > 3)) then 1 else 0 end as third_2013,
case when (count2014 > 0 AND (count2012 + count2013 + count2014) = 3) or ((count2012 + count2013) < 3 and (count2012 + count2013 + count2014) > 3) then 1 else 0 end as third_2014,
case when (count2015 > 0 AND (count2012 + count2013 + count2014 + count2015) = 3) or ((count2012 + count2013 + count2014) < 3 and (count2012 + count2013 + count2014 + count2015) > 3) then 1 else 0 end as third_2015,
case when (count2016 > 0 AND (count2012 + count2013 + count2014 + count2015 + count2016) = 3) or ((count2012 + count2013 + count2014 + count2015) < 3 and (count2012 + count2013 + count2014 + count2015 + count2016) > 3) then 1 else 0 end as third_2016,
case when (count2017 > 0 AND (count2012 + count2013 + count2014 + count2015 + count2016 + count2017) = 3) or ((count2012 + count2013 + count2014 + count2015 + count2016) < 3 and (count2012 + count2013 + count2014 + count2015 + count2016 + count2017) > 3) then 1 else 0 end as third_2017,
case when (count2018 > 0 AND (count2012 + count2013 + count2014 + count2015 + count2016 + count2017 + count2018) = 3) or ((count2012 + count2013 + count2014 + count2015 + count2016 + count2017) < 3 and (count2012 + count2013 + count2014 + count2015 + count2016 + count2017 + count2018) > 3) then 1 else 0 end as third_2018,
case when count2012 > 3 then 1 else 0 end as fourth_2012,
case when (count2013 > 0 AND (count2012 + count2013) = 4) or (((count2012) < 4) and ((count2012 + count2013) > 4)) then 1 else 0 end as fourth_2013,
case when (count2014 > 0 AND (count2012 + count2013 + count2014) = 4) or ((count2012 + count2013) < 4 and (count2012 + count2013 + count2014) > 4) then 1 else 0 end as fourth_2014,
case when (count2015 > 0 AND (count2012 + count2013 + count2014 + count2015) = 4) or ((count2012 + count2013 + count2014) < 4 and (count2012 + count2013 + count2014 + count2015) > 4) then 1 else 0 end as fourth_2015,
case when (count2016 > 0 AND (count2012 + count2013 + count2014 + count2015 + count2016) = 4) or ((count2012 + count2013 + count2014 + count2015) < 4 and (count2012 + count2013 + count2014 + count2015 + count2016) > 4) then 1 else 0 end as fourth_2016,
case when (count2017 > 0 AND (count2012 + count2013 + count2014 + count2015 + count2016 + count2017) = 4) or ((count2012 + count2013 + count2014 + count2015 + count2016) < 4 and (count2012 + count2013 + count2014 + count2015 + count2016 + count2017) > 4) then 1 else 0 end as fourth_2017,
case when (count2018 > 0 AND (count2012 + count2013 + count2014 + count2015 + count2016 + count2017 + count2018) = 4) or ((count2012 + count2013 + count2014 + count2015 + count2016 + count2017) < 4 and (count2012 + count2013 + count2014 + count2015 + count2016 + count2017 + count2018) > 4) then 1 else 0 end as fourth_2018,
case when count2012 > 4 then 1 else 0 end as fifth_2012,
case when (count2013 > 0 AND (count2012 + count2013) = 5) or (((count2012) < 5) and ((count2012 + count2013) > 5)) then 1 else 0 end as fifth_2013,
case when (count2014 > 0 AND (count2012 + count2013 + count2014) = 5) or ((count2012 + count2013) < 5 and (count2012 + count2013 + count2014) > 5) then 1 else 0 end as fifth_2014,
case when (count2015 > 0 AND (count2012 + count2013 + count2014 + count2015) = 5) or ((count2012 + count2013 + count2014) < 5 and (count2012 + count2013 + count2014 + count2015) > 5) then 1 else 0 end as fifth_2015,
case when (count2016 > 0 AND (count2012 + count2013 + count2014 + count2015 + count2016) = 5) or ((count2012 + count2013 + count2014 + count2015) < 5 and (count2012 + count2013 + count2014 + count2015 + count2016) > 5) then 1 else 0 end as fifth_2016,
case when (count2017 > 0 AND (count2012 + count2013 + count2014 + count2015 + count2016 + count2017) = 5) or ((count2012 + count2013 + count2014 + count2015 + count2016) < 5 and (count2012 + count2013 + count2014 + count2015 + count2016 + count2017) > 5) then 1 else 0 end as fifth_2017,
case when (count2018 > 0 AND (count2012 + count2013 + count2014 + count2015 + count2016 + count2017 + count2018) = 5) or ((count2012 + count2013 + count2014 + count2015 + count2016 + count2017) < 5 and (count2012 + count2013 + count2014 + count2015 + count2016 + count2017 + count2018) > 5) then 1 else 0 end as fifth_2018,
case when count2012 > 5 then 1 else 0 end as sixth_2012,
case when (count2013 > 0 AND (count2012 + count2013) = 6) or (((count2012) < 6) and ((count2012 + count2013) > 6)) then 1 else 0 end as sixth_2013,
case when (count2014 > 0 AND (count2012 + count2013 + count2014) = 6) or ((count2012 + count2013) < 6 and (count2012 + count2013 + count2014) > 6) then 1 else 0 end as sixth_2014,
case when (count2015 > 0 AND (count2012 + count2013 + count2014 + count2015) = 6) or ((count2012 + count2013 + count2014) < 6 and (count2012 + count2013 + count2014 + count2015) > 6) then 1 else 0 end as sixth_2015,
case when (count2016 > 0 AND (count2012 + count2013 + count2014 + count2015 + count2016) = 6) or ((count2012 + count2013 + count2014 + count2015) < 6 and (count2012 + count2013 + count2014 + count2015 + count2016) > 6) then 1 else 0 end as sixth_2016,
case when (count2017 > 0 AND (count2012 + count2013 + count2014 + count2015 + count2016 + count2017) = 6) or ((count2012 + count2013 + count2014 + count2015 + count2016) < 6 and (count2012 + count2013 + count2014 + count2015 + count2016 + count2017) > 6) then 1 else 0 end as sixth_2017,
case when (count2018 > 0 AND (count2012 + count2013 + count2014 + count2015 + count2016 + count2017 + count2018) = 6) or ((count2012 + count2013 + count2014 + count2015 + count2016 + count2017) < 6 and (count2012 + count2013 + count2014 + count2015 + count2016 + count2017 + count2018) > 6) then 1 else 0 end as sixth_2018,
case when count2012 > 6 then 1 else 0 end as seventh_2012,
case when (count2013 > 0 AND (count2012 + count2013) = 7) or (((count2012) < 7) and ((count2012 + count2013) > 7)) then 1 else 0 end as seventh_2013,
case when (count2014 > 0 AND (count2012 + count2013 + count2014) = 7) or ((count2012 + count2013) < 7 and (count2012 + count2013 + count2014) > 7) then 1 else 0 end as seventh_2014,
case when (count2015 > 0 AND (count2012 + count2013 + count2014 + count2015) = 7) or ((count2012 + count2013 + count2014) < 7 and (count2012 + count2013 + count2014 + count2015) > 7) then 1 else 0 end as seventh_2015,
case when (count2016 > 0 AND (count2012 + count2013 + count2014 + count2015 + count2016) = 7) or ((count2012 + count2013 + count2014 + count2015) < 7 and (count2012 + count2013 + count2014 + count2015 + count2016) > 7) then 1 else 0 end as seventh_2016,
case when (count2017 > 0 AND (count2012 + count2013 + count2014 + count2015 + count2016 + count2017) = 7) or ((count2012 + count2013 + count2014 + count2015 + count2016) < 7 and (count2012 + count2013 + count2014 + count2015 + count2016 + count2017) > 7) then 1 else 0 end as seventh_2017,
case when (count2018 > 0 AND (count2012 + count2013 + count2014 + count2015 + count2016 + count2017 + count2018) = 7) or ((count2012 + count2013 + count2014 + count2015 + count2016 + count2017) < 7 and (count2012 + count2013 + count2014 + count2015 + count2016 + count2017 + count2018) > 7) then 1 else 0 end as seventh_2018
FROM (
SELECT
	distinct
	users.id,
	COALESCE (c12.num, b12.num,0) as count2012,
	COALESCE (c13.num, b13.num,0) as count2013,
	COALESCE (c14.num, b14.num,0) as count2014,
	COALESCE (c15.num, b15.num,0) as count2015,
	COALESCE (c16.num, b16.num,0) as count2016,
	COALESCE (c17.num, b17.num,0) as count2017,
	COALESCE (c18.num, b18.num,0) as count2018
FROM users
JOIN (SELECT distinct user_id, move_plan_id FROM ownerships) as ownerships
	ON users.id = ownerships.user_id
LEFT JOIN(
SELECT ownerships.user_id, count(*) as num, date_part('year', c12.created_at) FROM jobs AS c12 JOIN ownerships ON ownerships.move_plan_id = c12.move_plan_id AND date_part('year', c12.created_at) = '2012' AND mover_state  = 'completed' GROUP BY ownerships.user_id, date_part('year', c12.created_at)
) as c12 ON c12.user_id = users.id
LEFT JOIN(
SELECT ownerships.user_id, 1        as num, date_part('year', b12.created_at) FROM jobs AS b12 JOIN ownerships ON ownerships.move_plan_id = b12.move_plan_id AND date_part('year', b12.created_at) = '2012' AND mover_state != 'completed' GROUP BY ownerships.user_id, date_part('year', b12.created_at)
) as b12 ON b12.user_id = users.id
LEFT JOIN(
SELECT ownerships.user_id, count(*) as num, date_part('year', c13.created_at) FROM jobs AS c13 JOIN ownerships ON ownerships.move_plan_id = c13.move_plan_id AND date_part('year', c13.created_at) = '2013' AND mover_state  = 'completed' GROUP BY ownerships.user_id, date_part('year', c13.created_at)
) as c13 ON c13.user_id = users.id
LEFT JOIN(
SELECT ownerships.user_id, 1        as num, date_part('year', b13.created_at) FROM jobs AS b13 JOIN ownerships ON ownerships.move_plan_id = b13.move_plan_id AND date_part('year', b13.created_at) = '2013' AND mover_state != 'completed' GROUP BY ownerships.user_id, date_part('year', b13.created_at)
) as b13 ON b13.user_id = users.id
LEFT JOIN(
SELECT ownerships.user_id, count(*) as num, date_part('year', c14.created_at) FROM jobs AS c14 JOIN ownerships ON ownerships.move_plan_id = c14.move_plan_id AND date_part('year', c14.created_at) = '2014' AND mover_state  = 'completed' GROUP BY ownerships.user_id, date_part('year', c14.created_at)
) as c14 ON c14.user_id = users.id
LEFT JOIN(
SELECT ownerships.user_id, 1        as num, date_part('year', b14.created_at) FROM jobs AS b14 JOIN ownerships ON ownerships.move_plan_id = b14.move_plan_id AND date_part('year', b14.created_at) = '2014' AND mover_state != 'completed' GROUP BY ownerships.user_id, date_part('year', b14.created_at)
) as b14 ON b14.user_id = users.id
LEFT JOIN(
SELECT ownerships.user_id, count(*) as num, date_part('year', c15.created_at) FROM jobs AS c15 JOIN ownerships ON ownerships.move_plan_id = c15.move_plan_id AND date_part('year', c15.created_at) = '2015' AND mover_state  = 'completed' GROUP BY ownerships.user_id, date_part('year', c15.created_at)
) as c15 ON c15.user_id = users.id
LEFT JOIN(
SELECT ownerships.user_id, 1        as num, date_part('year', b15.created_at) FROM jobs AS b15 JOIN ownerships ON ownerships.move_plan_id = b15.move_plan_id AND date_part('year', b15.created_at) = '2015' AND mover_state != 'completed' GROUP BY ownerships.user_id, date_part('year', b15.created_at)
) as b15 ON b15.user_id = users.id
LEFT JOIN(
SELECT ownerships.user_id, count(*) as num, date_part('year', c16.created_at) FROM jobs AS c16 JOIN ownerships ON ownerships.move_plan_id = c16.move_plan_id AND date_part('year', c16.created_at) = '2016' AND mover_state  = 'completed' GROUP BY ownerships.user_id, date_part('year', c16.created_at)
) as c16 ON c16.user_id = users.id
LEFT JOIN(
SELECT ownerships.user_id, 1        as num, date_part('year', b16.created_at) FROM jobs AS b16 JOIN ownerships ON ownerships.move_plan_id = b16.move_plan_id AND date_part('year', b16.created_at) = '2016' AND mover_state != 'completed' GROUP BY ownerships.user_id, date_part('year', b16.created_at)
) as b16 ON b16.user_id = users.id
LEFT JOIN(
SELECT ownerships.user_id, count(*) as num, date_part('year', c17.created_at) FROM jobs AS c17 JOIN ownerships ON ownerships.move_plan_id = c17.move_plan_id AND date_part('year', c17.created_at) = '2017' AND mover_state  = 'completed' GROUP BY ownerships.user_id, date_part('year', c17.created_at)
) as c17 ON c17.user_id = users.id
LEFT JOIN(
SELECT ownerships.user_id, 1        as num, date_part('year', b17.created_at) FROM jobs AS b17 JOIN ownerships ON ownerships.move_plan_id = b17.move_plan_id AND date_part('year', b17.created_at) = '2017' AND mover_state != 'completed' GROUP BY ownerships.user_id, date_part('year', b17.created_at)
) as b17 ON b17.user_id = users.id
LEFT JOIN(
SELECT ownerships.user_id, count(*) as num, date_part('year', c18.created_at) FROM jobs AS c18 JOIN ownerships ON ownerships.move_plan_id = c18.move_plan_id AND date_part('year', c18.created_at) = '2018' AND mover_state  = 'completed' GROUP BY ownerships.user_id, date_part('year', c18.created_at)
) as c18 ON c18.user_id = users.id
LEFT JOIN(
SELECT ownerships.user_id, 1        as num, date_part('year', b18.created_at) FROM jobs AS b18 JOIN ownerships ON ownerships.move_plan_id = b18.move_plan_id AND date_part('year', b18.created_at) = '2018' AND mover_state != 'completed' GROUP BY ownerships.user_id, date_part('year', b18.created_at)
) as b18 ON b18.user_id = users.id
) as counts
WHERE count2012 != 0 or count2013 != 0 or count2014 != 0 or count2015 != 0 or count2016 != 0 or count2017 != 0 or count2018 != 0 ORDER BY id
) as chungus GROUP BY COAL