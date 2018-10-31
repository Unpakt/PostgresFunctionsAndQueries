CREATE OR REPLACE function safe_movers_count_and_min_price(move_plan_param CHARACTER varying)
	returns numeric[] AS $func$
BEGIN
  RETURN (SELECT ARRAY[cast(count(*) as numeric), min(total_cost)] FROM comparison_presenter_v4(move_plan_param));
EXCEPTION WHEN PLPGSQL_ERROR THEN
	RETURN ARRAY[0,NULL];
END
$func$ LANGUAGE plpgsql;
SELECT * FROm safe_movers_count_and_min_price('b4de6140-6ea4-11e8-4d87-31e027a6f5f1');
SELECT ARRAY[cast(count(*) as numeric), min(total_cost)] FROM comparison_presenter_v4('b4de6140-6ea4-11e8-4d87-31e027a6f5f1')