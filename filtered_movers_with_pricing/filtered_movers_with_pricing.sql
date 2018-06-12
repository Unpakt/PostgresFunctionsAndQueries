SELECT * FROM potential_movers('4451bb62-6e62-11e8-98af-95c136308632');
SELECT * FROM filtered_movers_with_pricing('4956be4c-3ff9-11e8-9ea1-0f461a27ccab',null,false,true);
SELECT * FROM filtered_movers_with_pricing('fe884282-547a-11e8-89ac-016ea2b9fd71',null,true);
SELECT * FROM filtered_movers_with_pricing('fe884282-547a-11e8-89ac-016ea2b9fd71','{894,1661,371,2658,2118,15,679}');
SELECT * FROM filtered_movers_with_pricing('4451bb62-6e62-11e8-98af-95c136308632');
SELECT * FROM filtered_movers_with_pricing('b939368e-0527-11e8-3db1-0f461a27ccab','{752}',false,true);
SELECT * FROM potential_movers('fe884282-547a-11e8-89ac-016ea2b9fd71');
SELECT * FROM distance_in_miles('"65658 Broadway", New York, NY, 10012','11377');
SELECT * FROM comparison_presenter_v4('fff76706-fd86-11e7-ac9d-41df8e0f4b38',null,true);

DROP FUNCTION IF EXISTS distance_in_miles(VARCHAR, VARCHAR);
CREATE FUNCTION distance_in_miles(pick_up VARCHAR, drop_off VARCHAR)
  RETURNS numeric AS
$func$
BEGIN
RETURN
CASE WHEN pick_up = drop_off THEN
	0
ELSE
	COALESCE(
  (SELECT distance_in_miles
  FROM driving_distances
  WHERE (pick_up_hash = pick_up AND drop_off_hash = drop_off)
  ORDER BY created_at DESC
  LIMIT 1),
  (SELECT distance_in_miles
  FROM driving_distances
  WHERE (pick_up_hash = drop_off AND drop_off_hash = pick_up)
  ORDER BY created_at DESC
  LIMIT 1))
END;
END
$func$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS filtered_movers_with_pricing(VARCHAR);
DROP FUNCTION IF EXISTS filtered_movers_with_pricing(VARCHAR, INTEGER);
DROP FUNCTION IF EXISTS filtered_movers_with_pricing(VARCHAR, INTEGER[], BOOLEAN);
DROP FUNCTION IF EXISTS filtered_movers_with_pricing(VARCHAR, INTEGER[], BOOLEAN, BOOLEAN);
CREATE FUNCTION filtered_movers_with_pricing(move_plan_param VARCHAR, mover_param INTEGER[] DEFAULT NULL, select_from_temp BOOLEAN DEFAULT false, for_bid BOOLEAN DEFAULT false)
RETURNS TABLE(
  total numeric, total_adjustments numeric,
  mover_cut numeric, unpakt_fee numeric,
  coupon_discount numeric, mover_special_discount numeric,
  twitter_discount numeric, facebook_discount numeric,
  subtotal numeric, adj_before numeric,
  moving_cost_adjusted numeric, travel_cost_adjusted numeric,
  recache_and_rerun boolean,
  special_handling_cost_adjusted numeric, storage_cost numeric,
  packing_cost_adjusted numeric, cardboard_cost_adjusted numeric,
  surcharge_cubic_feet_cost_adjusted numeric,
  coi_charges_cost numeric, size_surcharge_cost_adjusted numeric,
  mover_name varchar, mover_id integer,
  pick_up_mileage numeric, drop_off_mileage numeric,
  extra_stop_enabled boolean,
  packing boolean,unpacking boolean, box_delivery boolean,
  piano boolean, storage boolean, onsites boolean,callback boolean,
  crating boolean,disassembly_assembly boolean,
  wall_dismounting boolean, box_delivery_range numeric,
  storage_in_transit boolean, warehousing boolean,
  local_cents_per_cubic_foot numeric, pu_lat numeric,
  pu_long numeric, latest_pc_id integer,mover_earth earth,
  local_consult_only boolean, interstate_consult_only boolean,
  mover_location_id integer,
  price_chart_id integer, cents_per_cubic_foot numeric,
  location_type varchar, maximum_delivery_days integer,
  minimum_delivery_days integer , dedicated boolean,
  extra_fee numeric, range numeric, partially_active boolean,
  location_latitude DOUBLE PRECISION, location_longitude DOUBLE PRECISION,
  distance_in_miles DOUBLE PRECISION, balancing_rate_primary NUMERIC,
  balancing_rate_secondary NUMERIC, net_am BIGINT, net_pm BIGINT, sit_avail BIGINT) AS $$

--DEFINE GENERAL VARIABLES
DECLARE mov_date date;DECLARE mov_time varchar;DECLARE sit_date date;
DECLARE mp_id integer;DECLARE mp_coupon_id integer;DECLARE num_stairs integer;
DECLARE total_cubic_feet numeric;DECLARE item_cubic_feet numeric;
DECLARE num_carpentry integer;DECLARE num_crating integer;
DECLARE box_dow integer;DECLARE box_date date;DECLARE box_cubic_feet numeric;
DECLARE frozen_pc_id integer; DECLARE frozen_mover_id integer;
DECLARE frozen_mover_latest_pc_id integer; DECLARE white_label_movers int[];
DECLARE commission numeric;

--DEFINE VARIABLES: PICKUP(pu_), EXTRA PICK UP(epu_), DROP OFF(do_), EXTRA DROP OFF(edo_)
DECLARE pu_state varchar; DECLARE pu_earth earth; DECLARE pu_key varchar;
DECLARE epu_state varchar;DECLARE epu_earth earth;DECLARE epu_key varchar;
DECLARE do_state varchar; DECLARE do_earth earth; DECLARE do_key varchar;
DECLARE edo_state varchar;DECLARE edo_earth earth;DECLARE edo_key varchar;

--DEFINE VARIABLES FOR LOOP
DECLARE adj RECORD;
DECLARE new_sub NUMERIC;
DECLARE old_total NUMERIC;
DECLARE new_total NUMERIC;
DECLARE before_adj NUMERIC;
DECLARE after_adj NUMERIC;
DECLARE mover_cut_sub NUMERIC;
DECLARE mover_cut_adj NUMERIC;
DECLARE unpakt_fee_sub NUMERIC;
DECLARE unpakt_fee_adj NUMERIC;


DECLARE
  BEGIN

    --SET GENERAL VARIABLES
    mp_id := (SELECT uuidable_id FROM uuids WHERE uuids.uuid = $1 AND uuidable_type = 'MovePlan');
    DROP TABLE IF EXISTS mp;
    CREATE TEMP TABLE mp AS (SELECT * FROM move_plans WHERE move_plans.id = mp_id);
    commission := (SELECT bid_commission_rate FROM mp);
    white_label_movers := (SELECT array_agg(white_label_whitelists.mover_id) FROM white_label_whitelists WHERE white_label_id = (SELECT white_label_id FROM mp));
    frozen_pc_id := COALESCE((SELECT jobs.price_chart_id FROM jobs WHERE mover_state <> 'declined' AND user_state NOT in('reserved_cancelled', 'cancelled') AND move_plan_id = mp_id LIMIT 1),(SELECT frozen_price_chart_id FROM mp));
    frozen_mover_id := (SELECT price_charts.mover_id FROM price_charts WHERE price_charts.id = frozen_pc_id);
    frozen_mover_latest_pc_id := (SELECT price_charts.id FROM price_charts WHERE price_charts.mover_id = frozen_mover_id ORDER BY created_at DESC LIMIT 1);
    mov_date := (SELECT move_date FROM mp);
    mov_time := (SELECT CASE WHEN mp.move_time LIKE '%PM%' THEN 'pm' ELSE 'am' END FROM mp );
    sit_date := (SELECT storage_move_out_date FROM mp);
    box_date := (SELECT box_delivery_date FROM mp);
    box_dow := (SELECT EXTRACT(isodow FROM box_date :: DATE));
    mp_coupon_id := (
      SELECT COALESCE(
          (SELECT coupon_id FROM jobs WHERE jobs.move_plan_id = mp_id AND user_state <> 'cancelled' AND mover_state <> 'declined' ORDER BY jobs.id LIMIT 1),
          (SELECT coupon_id FROM jobs WHERE jobs.move_plan_id = mp_id AND user_state = 'cancelled' ORDER BY jobs.id DESC LIMIT 1)));
		before_adj := 0.00;
		after_adj := 0.00;
		unpakt_fee_sub := 0.00;
		mover_cut_sub := 0.00;
		unpakt_fee_adj := 0.00;
		mover_cut_adj := 0.00;
	    --FIND MOVE PLAN INVENTORY ITEMS
	    DROP TABLE IF EXISTS mp_ii;
	    CREATE TEMP TABLE mp_ii AS (
	      SELECT
	        move_plan_inventory_items.id as mpii_id,
	        move_plan_id, inventory_item_id, item_group_id,
	        assembly_required, wall_removal_required, crating_required, is_user_selected, requires_piano_services,
	        inventory_items.name as item_name, icon_css_class, cubic_feet, is_user_generated, description
	      FROM move_plan_inventory_items
	      JOIN inventory_items
	      ON move_plan_inventory_items.inventory_item_id = inventory_items.id
	      AND move_plan_id = mp_id);

	    --FIND MOVE PLAN BOX ITEMS
	    DROP TABLE IF EXISTS mp_bi;
	    CREATE TEMP TABLE mp_bi AS (
	      SELECT
	        mpbi.id as mpbi_id,
	        move_plan_id, box_type_id, quantity,
	        cubic_feet
	      FROM box_inventories as mpbi
	      JOIN box_types AS bt
	      ON mpbi.move_plan_id = mp_id
	      AND mpbi.box_type_id = bt.id);

	    --FIND MOVE PLAN BOX ITEMS
	    DROP TABLE IF EXISTS mp_bp;
	    CREATE TEMP TABLE mp_bp AS (
	      SELECT
	        mpbp.id as mpbp_id,
	        move_plan_id, box_type_id, quantity,
	        cubic_feet
	      FROM box_purchases as mpbp
	      JOIN box_types AS bt
	      ON mpbp.move_plan_id = mp_id
	      AND mpbp.box_type_id = bt.id
	      AND quantity > 0);

	    --FIND CRATING ITEMS CUBIC FEET
	    DROP TABLE IF EXISTS crating_item_cubic_feet;
	    CREATE TEMP TABLE crating_item_cubic_feet AS(
	      SELECT COALESCE(cubic_feet,0) as cubic_feet
	      FROM mp_ii WHERE crating_required = true
	    );

	    --FIND TOTAL ITEM CUBIC FEET
	    item_cubic_feet := (SELECT COALESCE(SUM(cubic_feet),0) FROM mp_ii);

	    --FIND TOTAL BOX CUBIC FEET
	    box_cubic_feet := (SELECT COALESCE((
	      SELECT SUM(COALESCE(cubic_feet * quantity,0))
	      FROM mp_bi
	      GROUP BY mp_bi.move_plan_id),0));

	    --FIND TOTAL CUBIC FEET
	    total_cubic_feet := (SELECT box_cubic_feet + item_cubic_feet);

	    --FIND NUMBER OF CRATING ITEMS
	    num_crating := (SELECT COALESCE(count(*),0) FROM crating_item_cubic_feet);

	    --FIND NUMBER OF 'CARPENTRY' ITEMS
	    num_carpentry := (SELECT COALESCE(count(*),0) FROM mp_ii WHERE wall_removal_required = true OR assembly_required = true);

	    --SET ADDRESS VARIABLES USING THIS STUPID METHOD BECAUSE THE RAILS DEVELOPERS CAN'T READ THE POSTGRES DOCUMENTATION
	    --https://www.postgresql.org/docs/current/static/sql-select.html Description #8
	    DROP TABLE IF EXISTS mp_addresses;
	    CREATE TEMP TABLE mp_addresses AS
	      (SELECT * FROM addresses WHERE move_plan_id = mp_id AND geocoded_address IS NOT NULL AND role_in_plan = 'pick_up' LIMIT 1) UNION ALL
	      (SELECT * FROM addresses WHERE move_plan_id = mp_id AND geocoded_address IS NOT NULL AND role_in_plan = 'extra_pick_up' LIMIT 1) UNION ALL
	      (SELECT * FROM addresses WHERE move_plan_id = mp_id AND geocoded_address IS NOT NULL AND role_in_plan = 'drop_off' LIMIT 1) UNION ALL
	      (SELECT * FROM addresses WHERE move_plan_id = mp_id AND geocoded_address IS NOT NULL AND role_in_plan = 'extra_drop_off' LIMIT 1);

	    --HANDLE WAREHOSUE DESTINATIONS
	    IF (SELECT warehouse_destination FROM mp) = TRUE THEN
	      DELETE FROM mp_addresses WHERE role_in_plan = 'drop_off';
	      DELETE FROM mp_addresses WHERE role_in_plan = 'extra_drop_off';
	    END IF;

	    --SET ADDRESS VARIABLES
    num_stairs := (
      SELECT sum(flights_of_stairs) FROM (
        SELECT
          heights.flights_of_stairs
        FROM mp_addresses
        JOIN heights
          ON mp_addresses.height_id = heights.id
          AND mp_addresses.move_plan_id = mp_id
          AND mp_addresses.role_in_plan IN ('drop_off', 'pick_up') ) as stairs);
	    pu_state := (SELECT state FROM mp_addresses WHERE role_in_plan = 'pick_up');
	    epu_state := (SELECT state FROM mp_addresses WHERE role_in_plan = 'extra_pick_up');
	    do_state := (SELECT state FROM mp_addresses WHERE role_in_plan = 'drop_off');
	    edo_state := (SELECT state FROM mp_addresses WHERE role_in_plan = 'extra_drop_off');
	    pu_earth := (SELECT * FROM ll_to_earth(
	        (SELECT latitude FROM mp_addresses where role_in_plan = 'pick_up'),
	        (SELECT longitude FROM mp_addresses where role_in_plan = 'pick_up')));
	    epu_earth := (SELECT * FROM ll_to_earth(
	        (SELECT latitude FROM mp_addresses where role_in_plan = 'extra_pick_up'),
	        (SELECT longitude FROM mp_addresses where role_in_plan = 'extra_pick_up')));
	    do_earth := (SELECT * FROM ll_to_earth(
	        (SELECT latitude FROM mp_addresses WHERE role_in_plan = 'drop_off'),
	        (SELECT longitude FROM mp_addresses WHERE role_in_plan = 'drop_off')));
	    edo_earth := (SELECT * FROM ll_to_earth(
	        (SELECT latitude FROM mp_addresses WHERE role_in_plan = 'extra_drop_off'),
	        (SELECT longitude FROM mp_addresses WHERE role_in_plan = 'extra_drop_off')));
	    pu_key  := (SELECT distance_cache_key FROM mp_addresses WHERE role_in_plan = 'pick_up');
	    epu_key := (SELECT distance_cache_key FROM mp_addresses WHERE role_in_plan = 'extra_pick_up');
	    do_key  := (SELECT distance_cache_key FROM mp_addresses WHERE role_in_plan = 'drop_off');
	    edo_key := (SELECT distance_cache_key FROM mp_addresses WHERE role_in_plan = 'extra_drop_off');


		IF select_from_temp = false THEN
	    --FIND LIVE MOVERS
	    DROP TABLE IF EXISTS potential_movers;
	    CREATE TEMP TABLE potential_movers AS SELECT
	        COALESCE(branch_properties.trade_name, branch_properties.name) as mover_name,
	        movers.id,
          latest_pc.latest_pc_id,
	        movers.local_consult_only,
	        movers.interstate_consult_only
	      FROM movers
	      JOIN branch_properties
	        ON branchable_id = movers.id
	        AND (CASE WHEN mover_param IS NOT NULL THEN
	             movers.id = any(mover_param)
	            ELSE 1=1 END)
	        AND (
	            (branchable_type = 'Mover'AND marketplace_status = 'live' AND (movers.is_hidden = false OR movers.id = any(white_label_movers)))
            OR
              for_bid = true)
	      JOIN (SELECT
	              id as latest_pc_id,
	              price_charts.mover_id AS pc_mover_id,
	              rank() OVER(
	                PARTITION BY price_charts.mover_id
	                ORDER BY created_at DESC) as rank
	            FROM public.price_charts) as latest_pc
	        ON pc_mover_id = movers.id
	        AND
		        CASE WHEN frozen_mover_id IS NULL THEN
		          latest_pc.rank = 1
		          ELSE
		          ((latest_pc.rank = 1 AND latest_pc.pc_mover_id <> frozen_mover_id) OR latest_pc.latest_pc_id = frozen_pc_id)
	          END;

      --RAISE NO MOVER FOUND ERROR
      IF (SELECT COUNT(*) FROM potential_movers) = 0 THEN
        RAISE EXCEPTION 'No eligible movers';
      END IF;



	    --FILTER BY HAUL TYPE, PICK UP DISTANCE, INTRA/INTER(STATE) CERTIFICATION, MAX CUBIC FEET, MINIMUM DISTANCE
	    DROP TABLE IF EXISTS movers_by_haul;
	    CREATE TEMP TABLE movers_by_haul (mover_name varchar, mover_id integer, pick_up_mileage numeric, drop_off_mileage numeric,
	    extra_stop_enabled boolean, packing boolean, unpacking boolean, box_delivery boolean, piano boolean, storage boolean,
	    onsites boolean, callback boolean, crating boolean, disassembly_assembly boolean, wall_dismounting boolean,
	    box_delivery_range numeric, storage_in_transit boolean, warehousing boolean, local_cents_per_cubic_foot numeric,
	    pu_lat numeric, pu_long numeric, latest_pc_id integer, mover_earth earth, local_consult_only boolean, interstate_consult_only boolean);

	    --INTERSTATE MOVES
	    IF (SELECT count(distinct(state)) FROM mp_addresses) > 1 THEN
	        INSERT INTO movers_by_haul SELECT
	          potential_movers.mover_name, potential_movers.id AS mover_id,
	          price_charts.range as pick_up_mileage, price_charts.drop_off_mileage,
	          price_charts.extra_stop_enabled,
	          additional_services.packing,additional_services.unpacking, additional_services.box_delivery,
	          additional_services.piano, additional_services.storage, additional_services.onsites,additional_services.callback,
	          additional_services.crating,additional_services.disassembly_assembly,
	          additional_services.wall_dismounting, price_charts.box_delivery_range,
	          storage_details.storage_in_transit, storage_details.warehousing,
	          price_charts.cents_per_cubic_foot as local_cents_per_cubic_foot,
	          price_charts.latitude as pu_lat,  price_charts.longitude as pu_long, potential_movers.latest_pc_id,
	          (SELECT * FROM ll_to_earth(price_charts.latitude, price_charts.longitude)) AS mover_earth,
	          potential_movers.local_consult_only, potential_movers.interstate_consult_only
	        FROM potential_movers
	          JOIN price_charts
	            ON price_charts.id = potential_movers.latest_pc_id

	            --INTERSTATE CERTIFICATION
	            AND price_charts.us_dot IS NOT NULL
	            AND price_charts.us_dot <> ''
	            AND price_charts.usa_interstate_moves = 't'

	            --PICK UP IN RANGE
	            AND (price_charts.range * 1609.34) >= (SELECT * FROM earth_distance(
	                ll_to_earth(price_charts.latitude, price_charts.longitude),
	                pu_earth))

	            --LESS THAN MAX CUBIC FEET
	            AND ((price_charts.max_cubic_feet IS NULL OR total_cubic_feet <= price_charts.max_cubic_feet) OR for_bid = true)
	          JOIN additional_services
	            ON additional_services.price_chart_id = price_charts.id
	          JOIN storage_details
	            ON storage_details.additional_services_id = additional_services.id;

	        --RAISE NO MOVER FOUND ERROR
	        IF (SELECT COUNT(*) FROM movers_by_haul) = 0 THEN
	          RAISE EXCEPTION 'No interstate movers support this pick up address or can carry such a large amount';
	        END IF;

	    --INTRASTATE MOVES
	    ELSEIF do_key IS NOT NULL THEN
	        INSERT INTO movers_by_haul SELECT
	          potential_movers.mover_name, potential_movers.id AS mover_id,
	          price_charts.range as pick_up_mileage, price_charts.drop_off_mileage,
	          price_charts.extra_stop_enabled,
	          additional_services.packing,additional_services.unpacking, additional_services.box_delivery,
	          additional_services.piano, additional_services.storage, additional_services.onsites,additional_services.callback,
	          additional_services.crating,additional_services.disassembly_assembly,
	          additional_services.wall_dismounting, price_charts.box_delivery_range,
	          storage_details.storage_in_transit, storage_details.warehousing,
	          price_charts.cents_per_cubic_foot as local_cents_per_cubic_foot,
	          price_charts.latitude as pu_lat,  price_charts.longitude as pu_long, potential_movers.latest_pc_id,
	          (SELECT * FROM ll_to_earth(price_charts.latitude, price_charts.longitude)) AS mover_earth,
	          potential_movers.local_consult_only, potential_movers.interstate_consult_only
	        FROM potential_movers
	          JOIN price_charts
	            ON price_charts.id = potential_movers.latest_pc_id

	            --INTRASTATE CERTIFICATION
	            AND price_charts.local_moves = true
	            AND (price_charts.state_authority_1_state = pu_state OR
	                 price_charts.state_authority_2_state = pu_state OR
	                 price_charts.state_authority_3_state = pu_state OR
	                 price_charts.state_authority_4_state = pu_state )

	            --PICK UP IN RANGE
	            AND (price_charts.range * 1609.34) >= (SELECT * FROM earth_distance(
	                ll_to_earth(price_charts.latitude, price_charts.longitude),
	                pu_earth))

	            --LESS THAN MAX CUBIC FEET
	            AND ((price_charts.max_cubic_feet IS NULL OR total_cubic_feet <= price_charts.max_cubic_feet) OR for_bid = true)
	          JOIN additional_services
	            ON additional_services.price_chart_id = price_charts.id
	          JOIN storage_details
	            ON storage_details.additional_services_id = additional_services.id;

	        --RAISE NO MOVER FOUND ERROR
	        IF (SELECT COUNT(*) FROM movers_by_haul) = 0 THEN
	          RAISE EXCEPTION 'No in state movers support this pick up address or can carry such a large amount';
	        END IF;
	            --CHECK FOR MINIMUM DISTANCE IN PA AND IL
	          IF pu_state in ('IL', 'PA') THEN
	            DELETE FROM movers_by_haul
	            WHERE movers_by_haul.mover_id
	            IN (SELECT movers_by_haul.mover_id FROM movers_by_haul
	              JOIN price_charts
	              ON price_charts.id = movers_by_haul.latest_pc_id
	              AND movers_by_haul.local_consult_only = FALSE
	              AND (price_charts.minimum_job_distance * 1609.34) >= (SELECT earth_distance(COALESCE(do_earth,movers_by_haul.mover_earth),pu_earth)));
	          END IF;

	        --RAISE NO MOVER FOUND ERROR
	        IF (SELECT COUNT(*) FROM movers_by_haul) = 0 THEN
	          RAISE EXCEPTION 'Moves in IL or PA must exceed the minimum distance';
	        END IF;
	    ELSE

	      --MIS ELIGIBILITY
	        INSERT INTO movers_by_haul SELECT
	          potential_movers.mover_name, potential_movers.id AS mover_id,
	          price_charts.range as pick_up_mileage, price_charts.drop_off_mileage,
	          price_charts.extra_stop_enabled,
	          additional_services.packing,additional_services.unpacking, additional_services.box_delivery,
	          additional_services.piano, additional_services.storage, additional_services.onsites,additional_services.callback,
	          additional_services.crating,additional_services.disassembly_assembly,
	          additional_services.wall_dismounting, price_charts.box_delivery_range,
	          storage_details.storage_in_transit, storage_details.warehousing,
	          price_charts.cents_per_cubic_foot as local_cents_per_cubic_foot,
	          price_charts.latitude as pu_lat,  price_charts.longitude as pu_long, potential_movers.latest_pc_id,
	          (SELECT * FROM ll_to_earth(price_charts.latitude, price_charts.longitude)) AS mover_earth,
	          potential_movers.local_consult_only, potential_movers.interstate_consult_only
	        FROM potential_movers
	          JOIN price_charts
	            ON price_charts.id = potential_movers.latest_pc_id
	          JOIN zip_codes
	            ON price_charts.zip = zip_codes.zip

	            --PICK UP IN RANGE
	            AND (price_charts.range * 1609.34) >= (SELECT * FROM earth_distance(
	                ll_to_earth(price_charts.latitude, price_charts.longitude),
	                pu_earth))

	            --LESS THAN MAX CUBIC FEET
	            AND (price_charts.max_cubic_feet IS NULL OR total_cubic_feet <= price_charts.max_cubic_feet)
	          JOIN additional_services
	            ON additional_services.price_chart_id = price_charts.id
	          JOIN storage_details
	            ON storage_details.additional_services_id = additional_services.id

	          --FILTERS TO HANDLE IN/OUT OF STATE
	          WHERE(
	            CASE WHEN zip_codes.state <> pu_state THEN
	            --INTERSTATE CERTIFICATION
	              price_charts.us_dot IS NOT NULL
	              AND price_charts.us_dot <> ''
	              AND price_charts.usa_interstate_moves = 't'
	            WHEN zip_codes.state = pu_state THEN

	            --INTRASTATE CERTIFICATION
	              price_charts.local_moves = true
	              AND (price_charts.state_authority_1_state = pu_state OR
	                  price_charts.state_authority_2_state = pu_state OR
	                  price_charts.state_authority_3_state = pu_state OR
	                  price_charts.state_authority_4_state = pu_state )
	--               AND (price_charts.minimum_job_distance * 1609.34) >= (SELECT (SELECT * FROM ll_to_earth(price_charts.latitude, price_charts.longitude)),pu_earth)
	            END);

	        --RAISE NO MOVER FOUND ERROR
	        IF (SELECT COUNT(*) FROM movers_by_haul) = 0 THEN
	          RAISE EXCEPTION 'No movers support this storage pick up address';
	        END IF;

	            --CHECK FOR MINIMUM DISTANCE IN PA AND IL
	          IF pu_state in ('IL', 'PA') THEN
	            DELETE FROM movers_by_haul
	            WHERE movers_by_haul.mover_id
	            IN (SELECT movers_by_haul.mover_id FROM movers_by_haul
	              JOIN price_charts
	              ON price_charts.id = movers_by_haul.latest_pc_id
	              AND movers_by_haul.local_consult_only = FALSE
	              AND (price_charts.minimum_job_distance * 1609.34) >= (SELECT earth_distance(COALESCE(do_earth,movers_by_haul.mover_earth),pu_earth)));
	          END IF;

	        --RAISE NO MOVER FOUND ERROR
	        IF (SELECT COUNT(*) FROM movers_by_haul) = 0 THEN
	          RAISE EXCEPTION 'Moves in IL or PA must exceed the minimum distance';
	        END IF;

	    END IF;

	    --FILTER BY EXTRA PICK UP
	    IF (SELECT extra_pick_up_enabled FROM mp) = true THEN
	      DELETE FROM movers_by_haul WHERE (SELECT * FROM earth_distance(epu_earth,movers_by_haul.mover_earth)) > movers_by_haul.pick_up_mileage * 1609.34;
	    END IF;

	    --FIND LOCAL MOVERS
	    DROP TABLE IF EXISTS mover_local_locations;
	    CREATE TEMP TABLE mover_local_locations AS (
	      SELECT CAST(NULL AS INT) as mover_location_id,
	        movers_by_haul.latest_pc_id as price_chart_id,
	        movers_by_haul.local_cents_per_cubic_foot as cents_per_cubic_foot,
	        CAST('local' AS VARCHAR) as location_type,
	        CAST(NULL AS INT) as maximum_delivery_days,
	        CAST(NULL AS INT) as minimum_delivery_days,
	        CAST(NULL AS BOOLEAN) as dedicated,
	        CAST(NULL AS NUMERIC) as extra_fee,
	        movers_by_haul.drop_off_mileage AS range,
	        CAST(NULL AS BOOLEAN) as partially_active,
	        movers_by_haul.pu_lat as location_latitude, movers_by_haul.pu_long as location_longitude,
	        earth_distance(movers_by_haul.mover_earth,do_earth)/1609.34 AS distance_in_miles
	      FROM movers_by_haul
	      WHERE (SELECT * FROM earth_distance(movers_by_haul.mover_earth, COALESCE(do_earth,pu_earth))) <= movers_by_haul.drop_off_mileage * 1609.34
	      AND (SELECT local_moves FROM price_charts WHERE movers_by_haul.latest_pc_id = price_charts.id ));

	    --FIND ALL LONG DISTANCE MOVER LOCATIONS
	    DROP TABLE IF EXISTS mover_state_locations;
	    CREATE TEMP TABLE mover_state_locations AS (
	      SELECT mover_locations.id as mover_location_id,
	        mover_locations.price_chart_id, mover_locations.cents_per_cubic_foot,
	        mover_locations.location_type, mover_locations.maximum_delivery_days,
	        mover_locations.minimum_delivery_days, mover_locations.dedicated,
	        mover_locations.extra_fee, mover_locations.range, mover_locations.partially_active,
	        CAST(NULL AS NUMERIC) as location_latitude, CAST(NULL AS NUMERIC) AS location_longitude,
	        CAST(NULL AS NUMERIC) AS distance_in_miles
	      FROM public.mover_locations
	      WHERE state_code in(do_state, edo_state)
	            AND mover_locations.active = true
	            AND mover_locations.location_type='state'
	            AND mover_locations.price_chart_id in (SELECT DISTINCT movers_by_haul.latest_pc_id from movers_by_haul)
	            AND mover_locations.price_chart_id not in (SELECT DISTINCT mover_local_locations.price_chart_id FROM mover_local_locations));

	    --FIND ALL FULL COVERAGE LOCATIONS
	    DROP TABLE IF EXISTS mover_full_state_locations;
	    CREATE TEMP TABLE mover_full_state_locations AS (
	      SELECT mover_state_locations.mover_location_id,
	        mover_state_locations.price_chart_id, mover_state_locations.cents_per_cubic_foot,
	        mover_state_locations.location_type, mover_state_locations.maximum_delivery_days,
	        mover_state_locations.minimum_delivery_days, mover_state_locations.dedicated,
	        mover_state_locations.extra_fee, mover_state_locations.range, mover_state_locations.partially_active,
	        CAST(NULL AS NUMERIC) as location_latitude, CAST(NULL AS NUMERIC) AS location_longitude,
	        CAST(NULL AS NUMERIC) AS distance_in_miles
	      FROM mover_state_locations
	      WHERE mover_state_locations.partially_active = false);

	    --FIND ALL PARTIAL COVERAGE LOCATIONS SELECTING THE LOCATION THAT IS CLOSEST AND ALSO WITHIN RANGE
	    DROP TABLE IF EXISTS mover_city_locations;
	    CREATE TEMP TABLE mover_city_locations AS (
	      SELECT closest_locations.id as mover_location_id,
	        closest_locations.price_chart_id, closest_locations.cents_per_cubic_foot,
	        closest_locations.location_type, closest_locations.maximum_delivery_days,
	        closest_locations.minimum_delivery_days, closest_locations.dedicated,
	        closest_locations.extra_fee, closest_locations.range, closest_locations.partially_active,
	        closest_locations.latitude as location_latitude, closest_locations.longitude AS location_longitude,
	        closest_locations.distance_in_miles
	      FROM (
	      SELECT * FROM (SELECT *, rank()
	        OVER (PARTITION BY all_valid_locations.price_chart_id ORDER BY earth_distance(ll_to_earth(latitude,longitude),(SELECT * FROM ll_to_earth(
	        (SELECT latitude FROM mp_addresses WHERE role_in_plan = 'drop_off'),
	        (SELECT longitude FROM mp_addresses WHERE role_in_plan = 'drop_off')))) ASC, all_valid_locations.cents_per_cubic_foot DESC) as rank
	       FROM
	      (SELECT mover_locations.*, earth_distance(ll_to_earth(latitude,longitude),(SELECT * FROM ll_to_earth(
	        (SELECT latitude FROM mp_addresses WHERE role_in_plan = 'drop_off'),
	        (SELECT longitude FROM mp_addresses WHERE role_in_plan = 'drop_off'))))/1609.34 AS distance_in_miles
	          FROM public.mover_locations
	            WHERE state_code IN ((SELECT state FROM mp_addresses WHERE role_in_plan = 'drop_off'), (SELECT state FROM mp_addresses WHERE role_in_plan = 'extra_drop_off'))
	            AND mover_locations.active = TRUE
	            AND mover_locations.location_type='city'
	            AND mover_locations.price_chart_id IN (SELECT DISTINCT mover_state_locations.price_chart_id FROM mover_state_locations WHERE mover_state_locations.partially_active = true)
	            AND mover_locations.price_chart_id NOT IN (SELECT DISTINCT mover_full_state_locations.price_chart_id FROM mover_full_state_locations)) AS all_valid_locations
	      WHERE all_valid_locations.distance_in_miles <= all_valid_locations.range) AS ranked_locations WHERE rank = 1) AS closest_locations);

	    --UNION LOCAL,STATE,CITY LOCATIONS
	    DROP TABLE IF EXISTS all_mover_locations;
	    CREATE TEMP TABLE all_mover_locations AS (
	      SELECT * FROM mover_local_locations
	      UNION ALL SELECT * FROM mover_full_state_locations
	      UNION ALL SELECT * FROM mover_city_locations);
	    DROP TABLE IF EXISTS movers_with_location;
	    CREATE TEMP TABLE movers_with_location AS
	      SELECT * FROM movers_by_haul  JOIN all_mover_locations on all_mover_locations.price_chart_id =  movers_by_haul.latest_pc_id;

	        --RAISE NO MOVER FOUND ERROR
	        IF (SELECT COUNT(*) FROM movers_with_location) = 0 THEN
	          RAISE EXCEPTION 'No movers support this drop off address';
	        END IF;

	    --FILTER BY EXTRA DROP OFF
	    IF (SELECT mp.extra_drop_off_enabled FROM mp) = true THEN
	      DELETE FROM movers_with_location WHERE movers_with_location.extra_stop_enabled = false;
	      DELETE FROM movers_with_location WHERE movers_with_location.location_type = 'local' AND earth_distance(movers_with_location.mover_earth,edo_earth)/1609.34 > movers_with_location.drop_off_mileage;
	      DELETE FROM movers_with_location WHERE movers_with_location.location_type = 'state' AND earth_distance(do_earth,edo_earth)/1609.34 > GREATEST(50.0,movers_with_location.drop_off_mileage);
	      DELETE FROM movers_with_location WHERE movers_with_location.location_type = 'city'  AND earth_distance(ll_to_earth(movers_with_location.location_latitude,movers_with_location.location_longitude),edo_earth)/1609.34 > movers_with_location.range;
	    END IF;

	        --RAISE NO MOVER FOUND ERROR
	        IF (SELECT COUNT(*) FROM movers_with_location) = 0 THEN
	          RAISE EXCEPTION 'No movers can support this extra drop off location';
	        END IF;

	    --FILTER BY EXTRA PICK UP
	    IF (SELECT mp.extra_pick_up_enabled FROM mp) = true THEN
	      DELETE FROM movers_with_location WHERE movers_with_location.extra_stop_enabled = false;
	    END IF;

	        --RAISE NO MOVER FOUND ERROR
	        IF (SELECT COUNT(*) FROM movers_with_location) = 0 THEN
	          RAISE EXCEPTION 'No movers can support this extra pick up location';
	        END IF;



	    --FILTERING BY PACKING SERVICE
	    IF (SELECT follow_up_packing_service_id FROM mp) = 1 OR (SELECT initial_packing_service_id FROM mp) = 1 THEN
	      DELETE FROM movers_with_location WHERE movers_with_location.packing = false;
	    END IF;

	        --RAISE NO MOVER FOUND ERROR
	        IF (SELECT COUNT(*) FROM movers_with_location) = 0 THEN
	          RAISE EXCEPTION 'No Movers can support packing';
	        END IF;

	    IF (SELECT follow_up_packing_service_id FROM mp) = 2 OR (SELECT initial_packing_service_id FROM mp) = 2 THEN
	      DELETE FROM movers_with_location WHERE movers_with_location.packing = false;
	      DELETE FROM movers_with_location WHERE movers_with_location.unpacking = false;
	    END IF;

	        --RAISE NO MOVER FOUND ERROR
	        IF (SELECT COUNT(*) FROM movers_with_location) = 0 THEN
	          RAISE EXCEPTION 'No movers can support support unpacking';
	        END IF;

	    --FILTER BY BOX DELIVERY
	    IF (SELECT box_delivery_date FROM mp) IS NOT NULL THEN
	      DELETE FROM movers_with_location WHERE movers_with_location.box_delivery = false;
	      DELETE FROM movers_with_location WHERE (earth_distance(pu_earth,movers_with_location.mover_earth)* 0.000621371) > movers_with_location.box_delivery_range;
	    END IF;

	        --RAISE NO MOVER FOUND ERROR
	        IF (SELECT COUNT(*) FROM movers_with_location) = 0 THEN
	          RAISE EXCEPTION 'No movers can support box delivery';
	        END IF;

	    --FILTER BY PIANO
	    IF (SELECT COUNT(*) FROM mp_ii WHERE requires_piano_services = TRUE) > 0 THEN
	      DELETE FROM movers_with_location WHERE movers_with_location.piano = false;
	    END IF;

	        --RAISE NO MOVER FOUND ERROR
	        IF (SELECT COUNT(*) FROM movers_with_location) = 0 THEN
	          RAISE EXCEPTION 'No movers can support delivery of a piano';
	        END IF;

	    --FILTER BY MIS
	    IF (SELECT COUNT(*) FROM mp_addresses WHERE role_in_plan = 'drop_off') = 0 THEN
	      DELETE FROM movers_with_location WHERE movers_with_location.warehousing = false OR movers_with_location.storage = false;
	    END IF;

	        --RAISE NO MOVER FOUND ERROR
	        IF (SELECT COUNT(*) FROM movers_with_location) = 0 THEN
	          RAISE EXCEPTION 'No movers can support moving into storage';
	        END IF;

	    --FILTER BY SIT
	    IF (SELECT storage_move_out_date FROM mp) IS NOT NULL THEN
	      DELETE FROM movers_with_location WHERE movers_with_location.storage_in_transit = false OR movers_with_location.storage = false;
	    END IF;

	        --RAISE NO MOVER FOUND ERROR
	        IF (SELECT COUNT(*) FROM movers_with_location) = 0 THEN
	          RAISE EXCEPTION 'No movers can support storage in transit';
	        END IF;



	--     --FILTER BY PHONE REQUEST
	--     IF (SELECT count(*) FROM onsite_requests WHERE move_plan_id = mp_id AND type = 'InHomeRequest') > 0 THEN
	--       DELETE FROM movers_with_location WHERE movers_with_location.onsites = false;
	--     END IF;
	--
	--         --RAISE NO MOVER FOUND ERROR
	--         IF (SELECT COUNT(*) FROM movers_with_location) = 0 THEN
	--           RAISE EXCEPTION 'No movers can support phone requests';
	--         END IF;
	--
	--     --FILTER BY ONSITE REQUEST
	--     IF (SELECT count(*) FROM onsite_requests WHERE move_plan_id = mp_id AND type = 'PhoneRequest') > 0 THEN
	--       DELETE FROM movers_with_location WHERE movers_with_location.callback = false;
	--     END IF;
	--
	--         --RAISE NO MOVER FOUND ERROR
	--         IF (SELECT COUNT(*) FROM movers_with_location) = 0 THEN
	--           RAISE EXCEPTION 'No movers can support onsite requests';
	--         END IF;

	    --FILTER BY CRATING
	    IF num_crating > 0 THEN
	      DELETE FROM movers_with_location WHERE movers_with_location.crating = false;
	    END IF;

	        --RAISE NO MOVER FOUND ERROR
	        IF (SELECT COUNT(*) FROM movers_with_location) = 0 THEN
	          RAISE EXCEPTION 'No movers can support crating';
	        END IF;

	    --FILTER BY DISSASEMBLY/ASSEMBLY
	    IF (SELECT count(*) FROM mp_ii WHERE assembly_required = TRUE) > 0 THEN
	      DELETE FROM movers_with_location WHERE movers_with_location.disassembly_assembly = false;
	    END IF;

	        --RAISE NO MOVER FOUND ERROR
	        IF (SELECT COUNT(*) FROM movers_with_location) = 0 THEN
	          RAISE EXCEPTION 'No movers can support dissasembly/assembly';
	        END IF;

	    --FILTER BY WALL REMOVAL
	    IF (SELECT count(*) FROM mp_ii WHERE wall_removal_required = TRUE) > 0 THEN
	      DELETE FROM movers_with_location WHERE movers_with_location.wall_dismounting = false;
	    END IF;

	        --RAISE NO MOVER FOUND ERROR
	        IF (SELECT COUNT(*) FROM movers_with_location) = 0 THEN
	          RAISE EXCEPTION 'No movers can support wall dismounting';
	        END IF;

	    --FILTER BY AVAILABILITY AND GET BALANCING RATE
	    DROP TABLE IF EXISTS movers_with_location_and_balancing_rate;
	    CREATE TEMP TABLE movers_with_location_and_balancing_rate AS (
	      SELECT * FROM
	        (SELECT
	          mwl.*,

	          --BALANCING RATE
	          COALESCE(daily.balancing_rate_primary, weekly.balancing_rate_primary) AS balancing_rate_primary,
	          COALESCE(daily.balancing_rate_secondary, weekly.balancing_rate_secondary) AS balancing_rate_secondary,

	          --AVAILABILITY CALCULATIONS
	          CASE WHEN mwl.price_chart_id = frozen_pc_id THEN
		          CASE WHEN COALESCE(frz_daily.capacity_secondary, frz_weekly.capacity_secondary) IS NULL
	            THEN COALESCE(frz_daily.capacity_primary, frz_weekly.capacity_primary) - COALESCE(am, 0) - COALESCE(pm, 0)
		          ELSE COALESCE(frz_daily.capacity_secondary, frz_weekly.capacity_secondary) - COALESCE(pm, 0)
	            END
	          ELSE
		          CASE WHEN COALESCE(daily.capacity_secondary, weekly.capacity_secondary) IS NULL
	            THEN COALESCE(daily.capacity_primary, weekly.capacity_primary) - COALESCE(am, 0) - COALESCE(pm, 0)
		          ELSE COALESCE(daily.capacity_secondary, weekly.capacity_secondary) - COALESCE(pm, 0)
	            END
	          END AS net_pm,
	          CASE WHEN mwl.price_chart_id = frozen_pc_id THEN
		          CASE WHEN COALESCE(frz_daily.capacity_secondary, frz_weekly.capacity_secondary) IS NULL
	            THEN COALESCE(frz_daily.capacity_primary, frz_weekly.capacity_primary) - COALESCE(am, 0) - COALESCE(pm, 0)
		          ELSE COALESCE(frz_daily.capacity_primary, frz_weekly.capacity_primary) - COALESCE(am, 0)
	            END
	          ELSE
		          CASE WHEN COALESCE(daily.capacity_secondary, weekly.capacity_secondary) IS NULL
	            THEN COALESCE(daily.capacity_primary, weekly.capacity_primary) - COALESCE(am, 0) - COALESCE(pm, 0)
		          ELSE COALESCE(daily.capacity_primary, weekly.capacity_primary) - COALESCE(am, 0)
	            END
	          END AS net_am,
	          CASE WHEN mwl.price_chart_id = frozen_pc_id THEN
		          CASE WHEN COALESCE(frz_daily_sit.capacity_secondary, frz_weekly_sit.capacity_secondary) IS NULL
	            THEN COALESCE(frz_daily_sit.capacity_primary, frz_weekly_sit.capacity_primary) - COALESCE(sit_am, 0) - COALESCE(sit_pm, 0)
		          ELSE COALESCE(frz_daily_sit.capacity_primary, frz_weekly_sit.capacity_primary) - COALESCE(sit_am, 0)
	            END
	          ELSE
		          CASE WHEN COALESCE(daily_sit.capacity_secondary, weekly_sit.capacity_secondary) IS NULL
	            THEN COALESCE(daily_sit.capacity_primary, weekly_sit.capacity_primary) - COALESCE(sit_am, 0) - COALESCE(sit_pm, 0)
		          ELSE COALESCE(daily_sit.capacity_primary, weekly_sit.capacity_primary) - COALESCE(sit_am, 0)
	            END
	          END AS sit_avail
	        FROM movers_with_location as mwl

	        --ADJUSTMENTS BY DATE
	        LEFT JOIN(
	           SELECT *
	           FROM daily_adjustments AS day_adj
	           JOIN daily_adjustment_data AS adj_data
	             ON day_adj.daily_adjustment_datum_id = adj_data.id
	             AND day_adj.day = mov_date) AS daily
	           ON daily.price_chart_id = mwl.price_chart_id

	        --ADJUSTMENTS BY WEEKDAY
	        LEFT JOIN(SELECT *
	           FROM daily_adjustment_rules AS rul_adj
	           JOIN daily_adjustment_data AS adj_data
	             ON rul_adj.daily_adjustment_datum_id = adj_data.id
	             AND rul_adj.weekday =
	               CASE WHEN EXTRACT(isodow FROM mov_date :: DATE) = 7 THEN
	                  0
	                ELSE
	                  EXTRACT(isodow FROM mov_date :: DATE)
	                END) AS weekly
	           ON weekly.price_chart_id = mwl.price_chart_id

	        --SIT ADJUSTMENTS BY DATE
	        LEFT JOIN(
	           SELECT *
	           FROM daily_adjustments AS day_adj
	           JOIN daily_adjustment_data AS adj_data
	             ON day_adj.daily_adjustment_datum_id = adj_data.id
	             AND day_adj.day = sit_date) AS daily_sit
	           ON daily_sit.price_chart_id = mwl.price_chart_id

	        --SIT ADJUSTMENTS BY WEEKDAY
	        LEFT JOIN(
	           SELECT *
	           FROM daily_adjustment_rules AS rul_adj
	           JOIN daily_adjustment_data AS adj_data
	             ON rul_adj.daily_adjustment_datum_id = adj_data.id
	             AND rul_adj.weekday =
	              CASE WHEN EXTRACT(isodow FROM sit_date  :: DATE) = 7 THEN
	                0
	              ELSE
	                EXTRACT(isodow FROM sit_date  :: DATE)
	              END) AS weekly_sit
	           ON weekly_sit.price_chart_id = mwl.price_chart_id

	        --ADJUSTMENTS BY DATE FOR FROZEN MOVER
	        LEFT JOIN(
	           SELECT *
	           FROM daily_adjustments AS day_adj
	           JOIN daily_adjustment_data AS adj_data
	             ON day_adj.daily_adjustment_datum_id = adj_data.id
	             AND day_adj.day = mov_date) AS frz_daily
	           ON frozen_pc_id = mwl.price_chart_id
	           AND frz_daily.price_chart_id = frozen_mover_latest_pc_id

	        --ADJUSTMENTS BY WEEKDAY FOR FROZEN MOVER
	        LEFT JOIN(SELECT *
	           FROM daily_adjustment_rules AS rul_adj
	           JOIN daily_adjustment_data AS adj_data
	             ON rul_adj.daily_adjustment_datum_id = adj_data.id
	             AND rul_adj.weekday =
	               CASE WHEN EXTRACT(isodow FROM mov_date :: DATE) = 7 THEN
	                  0
	                ELSE
	                  EXTRACT(isodow FROM mov_date :: DATE)
	                END) AS frz_weekly
	           ON frozen_pc_id = mwl.price_chart_id
	           AND frz_weekly.price_chart_id = frozen_mover_latest_pc_id

	        --SIT ADJUSTMENTS BY DATE FOR FROZEN MOVER
	        LEFT JOIN(
	           SELECT *
	           FROM daily_adjustments AS day_adj
	           JOIN daily_adjustment_data AS adj_data
	             ON day_adj.daily_adjustment_datum_id = adj_data.id
	             AND day_adj.day = sit_date) AS frz_daily_sit
	           ON frozen_pc_id = mwl.price_chart_id
	           AND frz_daily_sit.price_chart_id = frozen_mover_latest_pc_id

	        --SIT ADJUSTMENTS BY WEEKDAY FOR FROZEN MOVER
	        LEFT JOIN(
	           SELECT *
	           FROM daily_adjustment_rules AS rul_adj
	           JOIN daily_adjustment_data AS adj_data
	             ON rul_adj.daily_adjustment_datum_id = adj_data.id
	             AND rul_adj.weekday =
	              CASE WHEN EXTRACT(isodow FROM sit_date  :: DATE) = 7 THEN
	                0
	              ELSE
	                EXTRACT(isodow FROM sit_date  :: DATE)
	              END) AS frz_weekly_sit
	           ON frozen_pc_id = mwl.price_chart_id
	           AND frz_weekly_sit.price_chart_id = frozen_mover_latest_pc_id

	        --MOVES BY MOVER ON MOVE DATE
	        LEFT JOIN(
	          SELECT
	            pc.mover_id,
	            move_date,
	            COUNT(CASE WHEN move_time NOT LIKE '%PM%' THEN move_time ELSE NULL END) AS am,
	            COUNT(CASE WHEN move_time LIKE '%PM%' THEN move_time ELSE NULL END) AS pm
	          FROM jobs AS jb
	          JOIN move_plans AS mp
	            ON mp.id = jb.move_plan_id
	            AND mover_state IN ('new', 'accepted', 'pending')
	            AND user_state <> 'cancelled'
	          JOIN price_charts AS pc
	            ON pc.id = jb.price_chart_id
	            GROUP BY move_date, pc.mover_id) AS jobs
	        ON mwl.mover_id = jobs.mover_id
	        AND mov_date = jobs.move_date

	        --MOVES BY MOVER ON SIT DATE
	        LEFT JOIN(
	          SELECT
	            pc.mover_id,
	            move_date,
	            COUNT(CASE WHEN move_time NOT LIKE '%PM%' THEN move_time ELSE NULL END) AS sit_am,
	            COUNT(CASE WHEN move_time LIKE '%PM%' THEN move_time ELSE NULL END) AS sit_pm
	          FROM jobs AS jb
	          JOIN move_plans AS mp
	            ON mp.id = jb.move_plan_id
	            AND mover_state IN ('new', 'accepted', 'pending')
	            AND user_state <> 'cancelled'
	          JOIN price_charts AS pc
	            ON pc.id = jb.price_chart_id
	            GROUP BY move_date, pc.mover_id) AS sit_jobs
	        ON mwl.mover_id = jobs.mover_id
	        AND sit_date = jobs.move_date
	        ) AS availability
	        WHERE
	        CASE
	          WHEN for_bid = TRUE THEN
	            1=1
	          WHEN mov_time = 'am' THEN
	            availability.net_am > 0
	          ELSE
	            availability.net_pm > 0
	        END
	    );

	        --RAISE NO MOVER FOUND ERROR
	        IF (SELECT COUNT(*) FROM movers_with_location_and_balancing_rate) = 0 THEN
	          RAISE EXCEPTION 'No movers are available on this move date';
	        END IF;

	      --FILTER BY SIT AVAILABILITY
	    IF sit_date IS NOT NULL AND for_bid = false THEN
	      DELETE FROM movers_with_location_and_balancing_rate WHERE movers_with_location_and_balancing_rate.sit_avail <= 0;
	    END IF;

	        --RAISE NO MOVER FOUND ERROR
	        IF (SELECT COUNT(*) FROM movers_with_location_and_balancing_rate) = 0 THEN
	          RAISE EXCEPTION 'No movers are available on this storage move out date';
	        END IF;
	  END IF;


    --PRECOMPUTE TRAVEL PLAN
    DROP TABLE IF EXISTS travel_plan_miles;
    CREATE TEMP TABLE travel_plan_miles AS
    (SELECT
      travel_plans.latest_pc_id,
      CASE WHEN travel_plans.distance_minus_free < 0 THEN
        0
      ELSE
        travel_plans.distance_minus_free
      END as distance_minus_free,
      CASE WHEN travel_plans.distance_minus_free + travel_plans.free_miles <= 0 THEN
        true
      ELSE
        false
      END as recache_and_rerun,
      travel_plans.free_miles as free_miles
      FROM
    (SELECT
        mwlabr.latest_pc_id,

        --WAREHOUSE TO PICK UP DISTANCE
      (COALESCE((SELECT * FROM distance_in_miles(pu_key,price_charts.distance_cache_key)),0.00) +

          --HANDLE LOCAL
        (CASE WHEN mwlabr.location_type = 'local' AND do_state IS NOT NULL AND (SELECT storage_move_out_date FROM mp) IS NULL THEN

            --HANDLE EXTRA PICK UP
          (CASE WHEN epu_state IS NOT NULL THEN

              --PICK UP TO EXTRA PICK UP DISTANCE
            COALESCE((SELECT * FROM distance_in_miles(pu_key,epu_key)),0.00) +

              --EXTRA PICK UP TO DROP OFF DISTANCE
            COALESCE((SELECT * FROM distance_in_miles(epu_key,do_key)),0.00)
          ELSE

              --PICK UP TO DROP OFF DISTANCE
            COALESCE((SELECT * FROM distance_in_miles(pu_key,do_key)),0.00)
          END) +

            --HANDLE EXTRA DROP OFF
          (CASE WHEN edo_state IS NOT NULL THEN

              --DROP OFF TO EXTRA DROP OFF DISTANCE
            COALESCE((SELECT * FROM distance_in_miles(do_key,edo_key)),0.00) +

              --EXTRA DROP OFF TO WAREHOUSE DISTANCE
            COALESCE((SELECT * FROM distance_in_miles(edo_key,price_charts.distance_cache_key)),0.00)
          ELSE

              --DROP OFF TO WAREHOUSE DISTANCE
            COALESCE((SELECT * FROM distance_in_miles(do_key,price_charts.distance_cache_key)),0.00)
          END)

          --HANDLE LOCAL SIT
        WHEN mwlabr.location_type = 'local' AND do_state IS NOT NULL AND (SELECT storage_move_out_date FROM mp) IS NOT NULL THEN

            --HANDLE EXTRA PICK UP
          (CASE WHEN epu_state IS NOT NULL THEN

              --PICK UP TO EXTRA PICK UP DISTANCE
            COALESCE((SELECT * FROM distance_in_miles(pu_key,epu_key)),0.00)+

              --EXTRA PICK UP TO WAREHOUSE DISTANCE
            COALESCE((SELECT * FROM distance_in_miles(epu_key,price_charts.distance_cache_key)),0.00)
          ELSE

              --PICK UP TO WAREHOUSE DISTANCE
            COALESCE((SELECT * FROM distance_in_miles(pu_key,price_charts.distance_cache_key)),0.00)
          END) +

            --HANDLE EXTRA DROP OFF
          (CASE WHEN edo_state IS NOT NULL THEN

              --WAREHOUSE TO DROP OFF DISTANCE
            COALESCE((SELECT * FROM distance_in_miles(price_charts.distance_cache_key,do_key)),0.00) +

              --DROP OFF TO EXTRA DROP OFF DISTANCE
            COALESCE((SELECT * FROM distance_in_miles(do_key,edo_key)),0.00) +

              --EXTRA DROP OFF TO WAREHOUSE DISTANCE
            COALESCE((SELECT * FROM distance_in_miles(edo_key,price_charts.distance_cache_key)),0.00)
          ELSE

              --DROP OFF TO WAREHOUSE DISTANCE
            COALESCE((SELECT * FROM distance_in_miles(do_key,price_charts.distance_cache_key)),0.00) * 2
          END)

          --SUBTRACT EXTRA FREE MILES FOR LOCAL SIT
          - price_charts.free_miles

      --HANDLE LONG DISTANCE AND MOVE INTO STORAGE
      ELSE
        (CASE WHEN epu_state IS NOT NULL THEN

            --PICK UP TO EXTRA PICK UP DISTANCE
          COALESCE((SELECT * FROM distance_in_miles(pu_key,epu_key)),0.00)+

            --EXTRA PICK UP TO WAREHOUSE DISTANCE
          COALESCE((SELECT * FROM distance_in_miles(epu_key,price_charts.distance_cache_key)),0.00)
        ELSE

            --PICK UP TO WAREHOUSE DISTANCE
          COALESCE((SELECT * FROM distance_in_miles(price_charts.distance_cache_key,pu_key)),0.00)
        END)
      END)

        --SUBTRACT FREE MILES FOR ALL MOVES
        - price_charts.free_miles) AS distance_minus_free,
        price_charts.free_miles as free_miles
       FROM movers_with_location_and_balancing_rate AS mwlabr
        JOIN price_charts
        ON mwlabr.latest_pc_id = price_charts.id
    ) AS travel_plans );

    --CRATING COST BY PRICE_CHART
    DROP TABLE IF EXISTS crating_cost_pc;
    CREATE TEMP TABLE crating_cost_pc AS (SELECT
      (CASE WHEN num_crating > 0 THEN
        SUM(GREATEST((cicf.cubic_feet * crating_pc.cents_per_cubic_foot_of_crating / 100.00),(crating_pc.minimum_cents_per_item_crated / 100.00)))
      ELSE
        0.00
      END) AS crating_cost,
      crating_pc.id AS crating_pc_id
    FROM crating_item_cubic_feet AS cicf
    JOIN price_charts AS crating_pc
    ON crating_pc.id IN (SELECT crating_mwlabr.latest_pc_id FROM movers_with_location_and_balancing_rate AS crating_mwlabr)
    GROUP BY crating_pc_id);

    --CARDBOARD COST BY PRICE_CHART
    DROP TABLE IF EXISTS cardboard_cost_pc;
    CREATE TEMP TABLE cardboard_cost_pc AS (SELECT

      --HANDLE BOX DELIVERY
      CASE WHEN (SELECT mp.box_delivery_date FROM MP) IS NOT NULL THEN
        SUM(cents_for_cardboard/100.00 * CAST(quantity AS NUMERIC)) +

        --FIGURE OUT RIDICULOUS BOX_DELIVERY_FEE (WHAT ARE THESE PATTERNS???????)
        (CASE box_dow
          WHEN 1 THEN COALESCE(cardboard_pc.box_delivery_fee_monday,0)
          WHEN 2 THEN COALESCE(cardboard_pc.box_delivery_fee_thursday,0)
          WHEN 3 THEN COALESCE(cardboard_pc.box_delivery_fee_wednesday,0)
          WHEN 4 THEN COALESCE(cardboard_pc.box_delivery_fee_thursday,0)
          WHEN 5 THEN COALESCE(cardboard_pc.box_delivery_fee_friday,0)
          WHEN 6 THEN COALESCE(cardboard_pc.box_delivery_fee_saturday,0)
          WHEN 7 THEN COALESCE(cardboard_pc.box_delivery_fee_sunday,0)
        ELSE 0.00 END)
      ELSE 0.00
      END AS cardboard_cost,

      --BALANCING RATE FOR BOX DELIVERY (THIS TRIGGERS ME)
      COALESCE(daily.balancing_rate_primary, weekly.balancing_rate_primary) AS box_balancing_rate_primary,
      COALESCE(daily.balancing_rate_secondary, weekly.balancing_rate_secondary) AS box_balancing_rate_secondary,
      cardboard_pc.id AS cardboard_pc_id
    FROM mp_bp
    JOIN price_charts AS cardboard_pc
    ON cardboard_pc.id IN (SELECT cb_p_up_mwlabr.latest_pc_id FROM movers_with_location_and_balancing_rate AS cb_p_up_mwlabr)
    JOIN box_type_rates AS btr
    ON cardboard_pc.id = btr.price_chart_id AND btr.box_type_id = mp_bp.box_type_id

    --BOX DELIVERY ADJUSTMENTS BY DATE
    LEFT JOIN(
       SELECT *
       FROM PUBLIC.daily_adjustments AS day_adj
       JOIN PUBLIC.daily_adjustment_data AS adj_data
         ON day_adj.daily_adjustment_datum_id = adj_data.id) AS daily
    ON box_date  = day
      AND cardboard_pc.id  = daily.price_chart_id

    --BOX DELIVERY ADJUSTMENTS BY WEEKDAY
    LEFT JOIN(
       SELECT *
       FROM PUBLIC.daily_adjustment_rules AS rul_adj
       JOIN PUBLIC.daily_adjustment_data AS adj_data
         ON rul_adj.daily_adjustment_datum_id = adj_data.id) AS weekly
    ON weekday =
       CASE WHEN box_dow = 7 THEN
          0
        ELSE
          box_dow
        END
    AND cardboard_pc.id  = weekly.price_chart_id
    GROUP BY
      cardboard_pc.id,
      COALESCE(daily.balancing_rate_primary, weekly.balancing_rate_primary),
      COALESCE(daily.balancing_rate_secondary, weekly.balancing_rate_secondary));

    --PACKING/UNPACKING COST BY PRICE_CHART
    DROP TABLE IF EXISTS packing_service_cost_pc;
    CREATE TEMP TABLE packing_service_cost_pc AS (SELECT

      --HANDLE PACKING COST (IF SELECTED)
      CASE WHEN (SELECT follow_up_packing_service_id FROM mp) = 1 OR (SELECT initial_packing_service_id FROM mp) = 1
                AND NOT ((SELECT follow_up_packing_service_id FROM mp) = 2 OR (SELECT initial_packing_service_id FROM mp) = 2)
                THEN SUM(cents_for_packing/100.00 * CAST(quantity AS NUMERIC))
      ELSE 0.00
      END AS packing_cost,

      --HANDLE UNPACKING COST (IF SELECTED)
      CASE WHEN (SELECT follow_up_packing_service_id FROM mp) = 2
                OR (SELECT initial_packing_service_id FROM mp) = 2
                THEN SUM(cents_for_unpacking/100.00 * CAST(quantity AS NUMERIC))
      ELSE 0.00
      END AS unpacking_cost,
      packing_service_pc.id AS packing_service_pc_id
    FROM mp_bi
    JOIN price_charts AS packing_service_pc
    ON packing_service_pc.id IN (SELECT cb_p_up_mwlabr.latest_pc_id FROM movers_with_location_and_balancing_rate AS cb_p_up_mwlabr)
    JOIN box_type_rates AS btr
    ON packing_service_pc.id = btr.price_chart_id AND btr.box_type_id = mp_bi.box_type_id
    GROUP BY
      packing_service_pc.packing_flat_fee,
      packing_service_pc.id);

    --GET MOVER SPECIAL DISCOUNTS
    DROP TABLE IF EXISTS mover_special_pc;
    CREATE TEMP TABLE mover_special_pc AS (
    SELECT *
    FROM mover_specials AS ms
    WHERE ms.price_chart_id IN (SELECT ms_mwlabr.latest_pc_id FROM movers_with_location_and_balancing_rate AS ms_mwlabr)
    AND active = true
    );

--DO ALL THE PRICING STUFF (oh boi)
DROP TABLE IF EXISTS movers_and_pricing_subtotal;
CREATE TEMP TABLE movers_and_pricing_subtotal AS (
      SELECT
        --SUBTOTAL
        pricing_data.moving_cost_adjusted +
        pricing_data.travel_cost_adjusted +
        pricing_data.special_handling_cost_adjusted +
        pricing_data.packing_cost_adjusted +
        pricing_data.storage_cost +
        pricing_data.cardboard_cost_adjusted +
        pricing_data.surcharge_cubic_feet_cost_adjusted +
        pricing_data.coi_charges_cost +
        pricing_data.size_surcharge_cost_adjusted AS subtotal,
        0.00 AS adj_before,
      pricing_data.*
      FROM (

        --MOVING COST ADJUSTED
        SELECT

          --ITEM COST
          ROUND(
            ((CASE WHEN mwlabr.location_type = 'local' AND (SELECT storage_move_out_date FROM mp) IS NOT NULL  THEN
              item_cubic_feet * mwlabr.local_cents_per_cubic_foot / 100.00 * 2.00
            ELSE
              item_cubic_feet * mwlabr.local_cents_per_cubic_foot / 100.00
            END) +

            --BOX COST
            (CASE WHEN mwlabr.location_type = 'local' AND (SELECT storage_move_out_date FROM mp) IS NOT NULL  THEN
              box_cubic_feet * mwlabr.local_cents_per_cubic_foot / 100.00 * 2.00
            ELSE
              box_cubic_feet * mwlabr.local_cents_per_cubic_foot / 100.00
            END) +

            --HEIGHT COST
           (total_cubic_feet * price_charts.cents_per_cubic_foot_per_flight_of_stairs * COALESCE(num_stairs,0.00) / 100.00) +

            --ITEM HANDLING COST
            (COALESCE(ihc.item_handling, 0.00)) +

            --BOX HANDLING COST
            (COALESCE(bhc.box_handling, 0.00))) *

            --MULTIPLY ABOVE BY BALANCING RATE
            balancing_rate.rate,
          2) AS moving_cost_adjusted,

          --TRAVEL COST ADJUSTED
          ROUND(

            --TRUCK COST
            ((
              (CASE WHEN mwlabr.location_type = 'local' AND (SELECT storage_move_out_date FROM mp) IS NOT NULL  THEN
                Cast(price_charts.cents_per_truck / 100.00 * 2.00 as numeric)
              ELSE
                Cast(price_charts.cents_per_truck / 100.00 as numeric)
              END) +

              --DISTANCE COST ADJUSTED
              (CASE WHEN travel_plan_miles.distance_minus_free < 0 THEN
                0.00
              ELSE
                travel_plan_miles.distance_minus_free * price_charts.cents_per_mile / 100.00
              END) +

              --HANDLE EXTRA LONG DISTANCE COSTS
              (CASE WHEN mwlabr.location_type = 'local' THEN
                0.00
              ELSE

                --LONG DISTANCE CUBIC FEET COST WITH PRICING TIERS
                ((total_cubic_feet * mwlabr.cents_per_cubic_foot / 100.00 * COALESCE(long_distance_tiers_coefficient,1.00) ) + COALESCE(mwlabr.extra_fee,0.00)) +

                --EXTRA DROP OFF LOCAL COST
                (CASE WHEN edo_state IS NULL THEN
                  0.00
                ELSE
                  (SELECT * FROM distance_in_miles(do_key,edo_key)) * price_charts.cents_per_mile / 100.00
                END)
              END) +

              --EXTRA PICK UP COST
                (CASE WHEN epu_state IS NULL THEN
                  0.00
                ELSE
                  COALESCE((price_charts.extra_stop_value / 100.00),0)
                END) +

              --EXTRA DROP OFF COST
                (CASE WHEN edo_state IS NULL THEN
                  0.00
                ELSE
                  COALESCE((price_charts.extra_stop_value / 100.00),0)
                END)

            ) *

            --MULTIPLY ABOVE BY BALANCING RATE
            balancing_rate.rate) +

              --INTERSTATE TOLL COST
              (CASE WHEN mwlabr.location_type = 'local' AND (SELECT COUNT(DISTINCT(state)) FROM mp_addresses) > 1 THEN
                price_charts.interstate_toll
              ELSE
                0.00
              END),
          2) AS travel_cost_adjusted,

        --ADD RECACHE AND RERUN COLUMN FOR BID MODEL
						travel_plan_miles.recache_and_rerun as recache_and_rerun,

        --SPECIAL HANDLING COST ADJUSTED

          --CRATING COST
           ROUND((COALESCE(crating_cost,0.00)  +

           --CARPENTRY COST
           (CASE WHEN num_carpentry > 0 THEN
             COALESCE((minimum_carpentry_cost_per_hour_in_cents / 100.00 * price_charts.special_handling_hours),0.00)
           ELSE
             0.00
           END
           ))* balancing_rate.rate, 2) AS special_handling_cost_adjusted,

        --STORAGE COST ADJUSTED
          ROUND(
          (CASE WHEN do_state IS NULL OR (SELECT storage_move_out_date FROM mp) IS NOT NULL THEN

            --CALCULATE MULTIPLIER
            (CASE WHEN do_state IS NULL AND (SELECT storage_move_out_date FROM mp) IS NULL THEN
              1.00
            WHEN DATE_PART('day', CAST((SELECT mp.storage_move_out_date FROM mp) AS timestamp) - CAST(mov_date AS timestamp)) <= 14 THEN
              0.50
            WHEN DATE_PART('day', CAST((SELECT mp.storage_move_out_date FROM mp) AS timestamp) - CAST(mov_date AS timestamp)) <= 31 THEN
              1.00
            ELSE
              1.50
            END) *

            --CALCULATE CUBIC FEET COST
            (CAST(price_charts.storage_fee AS NUMERIC)/ 100.00 * total_cubic_feet)
          ELSE
            0.00
          END),2) AS storage_cost,

        --PACKING COST ADJUSTED
          ROUND((((COALESCE(packing_service_cost_pc.packing_cost,0)) +
                  (COALESCE(packing_service_cost_pc.unpacking_cost,0)) +
                  (CASE WHEN (SELECT follow_up_packing_service_id FROM mp) IN (1,2) OR (SELECT initial_packing_service_id FROM mp) IN (1,2) THEN
                    price_charts.packing_flat_fee
                  ELSE
                    0.00
                  END)
                 ) *
                balancing_rate.rate),2) AS packing_cost_adjusted,

        --CARDBOARD COST ADJUSTED
          ROUND(((COALESCE(cardboard_cost_pc.cardboard_cost,0)) *

            --BALANCING RATE ON BOX DELIVERY DAY (ICKY)
           (1.00 + (
            (CASE WHEN mov_time = 'am' THEN
              coalesce(cardboard_cost_pc.box_balancing_rate_primary,0.00)
              ELSE
              coalesce(cardboard_cost_pc.box_balancing_rate_secondary, cardboard_cost_pc.box_balancing_rate_primary,0.00)
              END
            ) / 100.00))),2) AS cardboard_cost_adjusted,

        --SURCHARGE CUBIC FEET COST ADJUSTED
          ROUND((

            --LOCAL SURCHARGE
            (CASE WHEN price_charts.minimum_local_cubic_feet > total_cubic_feet THEN
              ((price_charts.minimum_local_cubic_feet - total_cubic_feet) * mwlabr.local_cents_per_cubic_foot / 100.00) *

              --LOCAL SIT MULTIPLIER
              (CASE WHEN mwlabr.location_type = 'local' AND (SELECT storage_move_out_date FROM mp) IS NOT NULL THEN
                2.00
              ELSE
                1.00
              END)
            ELSE
              0.00
            END) +

            --LONG DISTANCE SURCHARGE
            (CASE WHEN mwlabr.location_type = 'local' THEN
              0.00
            ELSE
              (CASE WHEN price_charts.minimum_long_distance_cubic_feet > total_cubic_feet THEN
                (price_charts.minimum_long_distance_cubic_feet - total_cubic_feet) * mwlabr.cents_per_cubic_foot / 100.00
              ELSE
                0.00
              END)
            END)) * balancing_rate.rate ,2) AS surcharge_cubic_feet_cost_adjusted,

        --COI CHARGES COST
          (SELECT COUNT(*) FROM mp_addresses where certificate_of_insurance_required = true) *
          coalesce(price_charts.coi_charge_cents/100.00,0.00) AS coi_charges_cost,

        --SIZE SURCHARGE COST ADJUSTED
          (CASE WHEN mwlabr.location_type = 'local' THEN
            ROUND(total_cubic_feet *
                  mwlabr.local_cents_per_cubic_foot/100.00 *
                  COALESCE(local_distance_tiers.local_tiers_coefficient, 0.00) *
                  balancing_rate.rate, 2)
           ELSE
            0.00
           END
          ) AS size_surcharge_cost_adjusted,
          mwlabr.*
        FROM movers_with_location_and_balancing_rate AS mwlabr

        --JOIN PRICE CHARTS FOR PRICING DATA
        JOIN price_charts
          ON mwlabr.latest_pc_id = price_charts.id

        --JOIN PRECOMPUTED TRAVEL PLAN DISTANCE
        JOIN travel_plan_miles
          ON mwlabr.latest_pc_id = travel_plan_miles.latest_pc_id

        --JOIN PRECOMPUTED CRATING COST
        LEFT JOIN crating_cost_pc
          ON crating_cost_pc.crating_pc_id = mwlabr.latest_pc_id

        --JOIN PRECOMPUTED BOX DELIVERY COSTS
        LEFT JOIN cardboard_cost_pc
          ON cardboard_cost_pc.cardboard_pc_id = mwlabr.latest_pc_id

        --JOIN PRECOMPUTED PACKING/UNPACKINT COSTS
        LEFT JOIN packing_service_cost_pc
          ON packing_service_cost_pc.packing_service_pc_id = mwlabr.latest_pc_id

        --JOIN BALANCING RATE
        LEFT JOIN (SELECT
          (1 + (
            (CASE WHEN mov_time = 'am' THEN
              mwlabr_br.balancing_rate_primary
              ELSE
              coalesce(mwlabr_br.balancing_rate_secondary, mwlabr_br.balancing_rate_primary)
              END
            ) / 100.00)) AS rate, mwlabr_br.latest_pc_id FROM movers_with_location_and_balancing_rate AS mwlabr_br) AS balancing_rate
        ON balancing_rate.latest_pc_id = mwlabr.latest_pc_id

        --JOIN ITEM HANDLING CHARGES
        LEFT JOIN (
            SELECT ihc.price_chart_id AS ihc_pc, sum(cost) AS item_handling
            FROM mp_ii
            JOIN item_handling_charges as ihc
            ON mp_ii.inventory_item_id = ihc.item_id
            AND ihc.item_type = 'InventoryItem'
            AND ihc.price_chart_id IN (SELECT DISTINCT items_mwlabr.price_chart_id FROM movers_with_location_and_balancing_rate AS items_mwlabr)
            GROUP BY ihc_pc) AS ihc
          ON ihc_pc = mwlabr.latest_pc_id

        --JOIN BOX HANDLING CHARGES
        LEFT JOIN (
            SELECT bhc.price_chart_id AS bhc_pc, sum(cost * quantity) AS box_handling
            FROM mp_bi
            JOIN item_handling_charges as bhc
            ON mp_bi.box_type_id = bhc.item_id
            AND bhc.item_type = 'BoxType'
            AND bhc.price_chart_id IN (SELECT DISTINCT boxes_mwlabr.price_chart_id FROM movers_with_location_and_balancing_rate AS boxes_mwlabr)
            GROUP BY bhc_pc) AS bhc
          ON bhc_pc = mwlabr.latest_pc_id

        --JOIN COMPUTED LONG DISTANCE TIERS
        LEFT JOIN (
            SELECT
              (SUM(rate_part) + 100.00)/100.00 AS long_distance_tiers_coefficient,
              pre_sum.price_chart_id AS long_distance_price_chart_id
            FROM(
              SELECT
              (CASE WHEN cubic_foot_max = MAX(cubic_foot_max) over (partition by cubic_feet_tier_long_distances.price_chart_id) AND total_cubic_feet < cubic_foot_max THEN
                total_cubic_feet
              ELSE
                cubic_foot_max
              END - cubic_foot_min) / total_cubic_feet * discount_percentage AS rate_part,
              cubic_feet_tier_long_distances.price_chart_id
              FROM cubic_feet_tier_long_distances
              WHERE cubic_foot_min < total_cubic_feet
              ORDER BY cubic_foot_max)
            AS pre_sum GROUP BY pre_sum.price_chart_id) AS long_distance_tiers
          ON long_distance_price_chart_id = mwlabr.latest_pc_id

        --JOIN COMPUTED LOCAL TIERS
        LEFT JOIN (
            SELECT
              SUM(rate_part)/100.00 AS local_tiers_coefficient,
              pre_sum.price_chart_id AS local_price_chart_id
            FROM(
              SELECT
              (CASE WHEN cubic_foot_max = MAX(cubic_foot_max) over (partition by cubic_feet_tier_locals.price_chart_id) AND total_cubic_feet < cubic_foot_max THEN
                total_cubic_feet
              ELSE
                cubic_foot_max
              END - cubic_foot_min) / total_cubic_feet * discount_percentage AS rate_part,
              cubic_feet_tier_locals.price_chart_id
              FROM cubic_feet_tier_locals
              WHERE cubic_foot_min < total_cubic_feet
              ORDER BY cubic_foot_max)
            AS pre_sum GROUP BY pre_sum.price_chart_id) AS local_distance_tiers
          ON local_price_chart_id = mwlabr.latest_pc_id
    ) AS pricing_data
  );



--HANDLE BEFORE DISCOUNT ADMIN ADJUSTMENTS
DROP TABLE IF EXISTS mp_admin_adjustments;
CREATE TEMP TABLE mp_admin_adjustments AS (
SELECT
  id,
  amount_in_cents,
  percentage,
  is_applied_before_discounts,
  applies_to,
  created_at
FROM admin_adjustments WHERE planable_id = mp_id AND planable_type = 'MovePlan'
);

IF
	(SELECT count(*) FROM mp_admin_adjustments WHERE is_applied_before_discounts = true) > 0
	AND for_bid = true
	AND (SELECT count(*) FROM movers_and_pricing_subtotal) = 1
THEN
	new_sub := (SELECT movers_and_pricing_subtotal.subtotal FROM movers_and_pricing_subtotal LIMIT 1);
	FOR adj IN SELECT * FROM mp_admin_adjustments WHERE is_applied_before_discounts = TRUE ORDER BY created_at ASC
	LOOP
		IF adj.percentage <> 0 AND adj.percentage IS NOT NULL THEN
			UPDATE admin_adjustments SET amount_in_cents = (adj.percentage * new_sub) WHERE id = adj.id;
			before_adj := before_adj + (adj.percentage * new_sub)/100.00;
			new_sub := new_sub + (adj.percentage * new_sub)/100.00;
		ELSE
			before_adj := before_adj + adj.amount_in_cents/100.00;
			new_sub := new_sub + adj.amount_in_cents/100.00;
		END IF;
		RAISE NOTICE '%', before_adj;
	END LOOP;
	UPDATE movers_and_pricing_subtotal SET subtotal = new_sub;
	UPDATE movers_and_pricing_subtotal SET adj_before = before_adj;
END IF;

DROP TABLE IF EXISTS movers_and_pricing;
CREATE TEMP TABLE movers_and_pricing AS (
  SELECT

  --TOTAL
    total.subtotal +
    total.mover_special_discount +
    total.facebook_discount +
    total.twitter_discount +
    CASE
      WHEN COALESCE((SELECT percentage FROM coupons WHERE mp_coupon_id = coupons.id AND active = TRUE ), false) = true THEN
          (SELECT discount_percentage FROM coupons WHERE mp_coupon_id = coupons.id AND active = TRUE ) *
          -1.00 / 100.00 *
          (total.subtotal + total.mover_special_discount)
      WHEN COALESCE((SELECT percentage FROM coupons WHERE mp_coupon_id = coupons.id AND active = TRUE ), true) = false THEN
          (SELECT discount_cents FROM coupons WHERE mp_coupon_id = coupons.id AND active = TRUE ) *
          -1.00 / 100.00
      ELSE
          0
      END AS total,
      total.adj_before AS total_adjustments,
      0.00 AS  mover_cut,
      0.00 AS unpakt_fee,
      --COUPON DISCOUNT
      CASE
      WHEN COALESCE((SELECT percentage FROM coupons WHERE mp_coupon_id = coupons.id AND active = TRUE ), false) = true THEN
          (SELECT discount_percentage FROM coupons WHERE mp_coupon_id = coupons.id AND active = TRUE ) *
          -1.00 / 100.00 *
          (total.subtotal + total.mover_special_discount)
      WHEN COALESCE((SELECT percentage FROM coupons WHERE mp_coupon_id = coupons.id AND active = TRUE ), true) = false THEN
          (SELECT discount_cents FROM coupons WHERE mp_coupon_id = coupons.id AND active = TRUE ) *
          -1.00 / 100.00
      ELSE
          0
      END AS coupon_discount,
    total.*
  FROM (
    SELECT

      --MOVER SPECIAL DISCOUNT
      COALESCE(ROUND((CASE WHEN subtotal.location_type = 'local' THEN
        (CASE WHEN (SELECT percentage FROM mover_special_pc AS mspc WHERE mspc.price_chart_id = subtotal.latest_pc_id AND short_haul = true LIMIT 1) = true THEN
            (SELECT discount_percentage * -1.00/100.00 FROM mover_special_pc AS mspc WHERE mspc.price_chart_id = subtotal.latest_pc_id AND short_haul = true LIMIT 1)
            * subtotal.subtotal
          ELSE
            (SELECT discount_cents * -1.00/100.00 FROM mover_special_pc AS mspc WHERE mspc.price_chart_id = subtotal.latest_pc_id AND short_haul = true LIMIT 1)
          END)
      ELSE
        (CASE WHEN (SELECT percentage FROM mover_special_pc AS mspc WHERE mspc.price_chart_id = subtotal.latest_pc_id AND long_haul = true LIMIT 1) = true THEN
          (SELECT discount_percentage * -1.00/100.00 FROM mover_special_pc AS mspc WHERE mspc.price_chart_id = subtotal.latest_pc_id AND long_haul = true LIMIT 1)
          * subtotal.subtotal
        ELSE
          (SELECT discount_cents * -1.00/100.00 FROM mover_special_pc AS mspc WHERE mspc.price_chart_id = subtotal.latest_pc_id AND long_haul = true LIMIT 1)
        END)
      END
      ),2),0.00) AS mover_special_discount,

      --TWITTER DISCOUNT
      CASE WHEN (SELECT mp.shared_on_twitter = true FROM mp) THEN
          -5.00
      ELSE
          0
      END AS twitter_discount,

      --FACEBOOK DISCOUNT
      CASE WHEN (SELECT mp.shared_on_facebook = true FROM mp) THEN
          -5.00
      ELSE
          0
      END AS facebook_discount,
      subtotal.*
    FROM movers_and_pricing_subtotal AS subtotal
) AS total);

IF
	(SELECT count(*) FROM mp_admin_adjustments WHERE is_applied_before_discounts = false AND applies_to = 'both') > 0
	AND for_bid = true
	AND (SELECT count(*) FROM movers_and_pricing) = 1
THEN
	new_total := (SELECT movers_and_pricing.total FROM movers_and_pricing LIMIT 1);
	old_total := new_total;
	FOR adj IN SELECT * FROM mp_admin_adjustments WHERE is_applied_before_discounts = false AND applies_to = 'both' ORDER BY created_at ASC
	LOOP
		IF adj.percentage <> 0 AND adj.percentage IS NOT NULL THEN
			UPDATE admin_adjustments SET amount_in_cents = (adj.percentage * new_total) WHERE id = adj.id;
			after_adj := after_adj + (adj.percentage * new_total)/100.00;
			new_total := new_total + (adj.percentage * new_total)/100.00;
		ELSE
			after_adj := after_adj + adj.amount_in_cents/100.00;
			new_total := new_total + adj.amount_in_cents/100.00;
		END IF;
		RAISE NOTICE 'after_adj = %', after_adj;
	END LOOP;
	UPDATE movers_and_pricing SET total = new_total;
	UPDATE movers_and_pricing SET total_adjustments = before_adj + after_adj;
END IF;

IF for_bid = true AND (SELECT count(*) FROM movers_and_pricing) = 1 THEN
	unpakt_fee_sub := old_total;
	mover_cut_sub := old_total;
	IF (SELECT count(*) FROM mp_admin_adjustments WHERE is_applied_before_discounts = false AND applies_to <> 'both') > 0 THEN
		FOR adj IN SELECT * FROM mp_admin_adjustments WHERE is_applied_before_discounts = false AND applies_to <> 'both' ORDER BY created_at ASC
		LOOP
			IF adj.percentage <> 0 AND adj.percentage IS NOT NULL THEN
				IF adj.applies_to = 'mover_fee' THEN
					UPDATE admin_adjustments SET amount_in_cents = (adj.percentage * mover_cut_sub) WHERE id = adj.id;
					mover_cut_adj := mover_cut_adj + (adj.percentage * mover_cut_sub)/100.00;
					mover_cut_sub := mover_cut_sub + (adj.percentage * mover_cut_sub)/100.00;
				ELSEIF adj.applies_to = 'unpakt_fee' THEN
					UPDATE admin_adjustments SET amount_in_cents = (adj.percentage * unpakt_fee_sub) WHERE id = adj.id;
					unpakt_fee_adj := unpakt_fee_adj + (adj.percentage * unpakt_fee_sub)/100.00;
					unpakt_fee_sub := unpakt_fee_sub + (adj.percentage * unpakt_fee_sub)/100.00;
				END IF;
			ELSE
				IF adj.applies_to = 'mover_fee' THEN
					mover_cut_adj := mover_cut_adj + adj.amount_in_cents/100.00;
					mover_cut_sub := mover_cut_sub + adj.amount_in_cents/100.00;
				ELSEIF adj.applies_to = 'unpakt_fee' THEN
					unpakt_fee_adj := unpakt_fee_adj + adj.amount_in_cents/100.00;
					unpakt_fee_sub := unpakt_fee_sub + adj.amount_in_cents/100.00;
				END IF;
			END IF;
			RAISE NOTICE 'mover_adj = %', after_adj;
			RAISE NOTICE 'unpakt_adj = %', after_adj;
		END LOOP;
	END IF;
	UPDATE movers_and_pricing SET mover_cut = GREATEST((((mp.subtotal + after_adj)*(1 - (commission/100.00))) + mp.mover_special_discount + mover_cut_adj),0.00) FROM movers_and_pricing AS mp ;
	UPDATE movers_and_pricing SET unpakt_fee = GREATEST((((mp.subtotal + after_adj)*(commission/100.00)) + mp.coupon_discount + mp.twitter_discount + mp.facebook_discount + unpakt_fee_adj),0.00) FROM movers_and_pricing AS mp;
END IF;

RETURN QUERY SELECT * FROM movers_and_pricing ORDER BY (
  (movers_and_pricing.local_consult_only OR movers_and_pricing.interstate_consult_only),movers_and_pricing.total
) ASC;
END; $$
LANGUAGE plpgsql;


DROP FUNCTION IF EXISTS comparison_presenter_v4(VARCHAR,INT[],BOOLEAN);
CREATE FUNCTION comparison_presenter_v4(move_plan_param VARCHAR, mover_param INTEGER[] DEFAULT NULL, select_from_temp BOOLEAN DEFAULT false)
  RETURNS TABLE(
  branch_property_id integer,
  city_state_label varchar,
  consult_only boolean,
  dedicated boolean,
  maximum_delivery_days integer,
  minimum_delivery_days integer,
  grade numeric,
  id integer,
  is_featured boolean,
  logo_url varchar,
  mover_special numeric,
  name varchar,
  number_of_employees integer,
  number_of_trucks integer,
  moving numeric,
  packing_cost numeric,
  special_handling_cost numeric,
  storage_cost numeric,
  profile_path varchar,
  google_link varchar,
  google_number_of_reviews integer,
  google_rating numeric,
  google_rounded_rating numeric,
  unpakt_link varchar,
  unpakt_number_of_reviews integer,
  unpakt_rating numeric,
  unpakt_rounded_rating numeric,
  yelp_link varchar,
  yelp_number_of_reviews integer,
  yelp_rating numeric,
  yelp_rounded_rating numeric,
  slug varchar,
  total_cost numeric,
  years_in_business integer
  ) AS
$func$
BEGIN
RETURN QUERY
(SELECT
	uniq.branch_property_id, uniq.city_state_label, uniq.consult_only, uniq.dedicated,
	uniq.maximum_delivery_days, uniq.minimum_delivery_days,	uniq.grade, uniq.id,
	uniq.is_featured, uniq.logo_url, uniq.mover_special, uniq.name, uniq.number_of_employees,
	uniq.number_of_trucks, uniq.moving, uniq.packing_cost, uniq.special_handling_cost,
	uniq.storage_cost, uniq.profile_path, uniq.google_link, uniq.google_number_of_reviews,
	uniq.google_rating, uniq.google_rounded_rating, uniq.unpakt_link, uniq.unpakt_number_of_reviews,
	uniq.unpakt_rating, uniq.unpakt_rounded_rating, uniq.yelp_link, uniq.yelp_number_of_reviews,
	uniq.yelp_rating, uniq.yelp_rounded_rating, uniq.slug, uniq.total_cost, uniq.years_in_business
FROM
	(SELECT
		*,
		rank() OVER(PARTITION BY all_lines.vendor_id ORDER BY all_lines.total_cost ASC)
	FROM
		(SELECT DISTINCT
        bp.id AS branch_property_id,
        CAST(ba.city || ', ' || ba.state AS VARCHAR) AS city_state_label,
        (CASE WHEN pricing.location_type = 'local' THEN
          pricing.local_consult_only
        ELSE
          pricing.interstate_consult_only
        END) AS consult_only,
        COALESCE(pricing.dedicated, 'f') as dedicated,
        pricing.maximum_delivery_days,
        pricing.minimum_delivery_days,
        ROUND(movers.numeric_grade + 78) AS grade,
        movers.id,
        movers.is_featured,
        bp.logo_image AS logo_url,
        -1.00 * pricing.mover_special_discount AS mover_special,
        COALESCE(bp.trade_name,bp.name) AS name,
        bp.vendor_id,
        movers.number_of_employees,
        movers.number_of_trucks,
        pricing.moving_cost_adjusted AS moving,
        pricing.packing_cost_adjusted AS packing_cost,
        pricing.special_handling_cost_adjusted AS special_handling_cost,
        pricing.storage_cost AS storage_cost,
        bp.slug AS profile_path,
        google.link AS google_link,
        google.number_of_reviews AS google_number_of_reviews,
        CAST(google.rating AS NUMERIC) AS google_rating,
        ROUND(ROUND(CAST(google.rating AS numeric) * 2.00)/2,1) AS google_rounded_rating,
        unpakt.link AS unpakt_link,
        unpakt.number_of_reviews AS unpakt_number_of_reviews,
        CAST(unpakt.rating AS NUMERIC) AS unpakt_rating,
        ROUND(ROUND(CAST(unpakt.rating AS numeric) * 2.00)/2,1) AS unpakt_rounded_rating,
        yelp.link AS yelp_link,
        yelp.number_of_reviews AS yelp_number_of_reviews,
        CAST(yelp.rating AS NUMERIC) AS yelp_rating,
        ROUND(ROUND(CAST(yelp.rating AS numeric) * 2.00)/2,1) AS yelp_rounded_rating,
        bp.slug AS slug,
        pricing.total AS total_cost,
        CAST(GREATEST((DATE_PART('year',now()) - COALESCE(bp.year_founded,DATE_PART('year',now()))),1) AS INTEGER) AS years_in_business
        FROM filtered_movers_with_pricing(move_plan_param,mover_param,select_from_temp) AS pricing
        JOIN movers ON movers.id = pricing.mover_id
        JOIN branch_properties AS bp ON bp.branchable_id = movers.id AND branchable_type = 'Mover'
        JOIN base_addresses AS ba ON bp.id = ba.branch_property_id
        JOIN service_provider_ratings AS yelp ON yelp.service_provider_id = movers.id AND yelp.service_provider_type = 'Mover'AND yelp.reviewer = 'Yelp'
        JOIN service_provider_ratings AS google ON google.service_provider_id = movers.id AND google.service_provider_type = 'Mover'AND google.reviewer = 'Google'
        JOIN service_provider_ratings AS unpakt ON unpakt.service_provider_id = movers.id AND unpakt.service_provider_type = 'Mover'AND unpakt.reviewer = 'Unpakt'
    ) as all_lines
  ) as uniq WHERE uniq.rank = 1
);
END
$func$ LANGUAGE plpgsql;