DROP FUNCTION IF EXISTS potential_movers(VARCHAR, INTEGER[]);
CREATE FUNCTION potential_movers(move_plan_param VARCHAR, mover_param INTEGER[] DEFAULT NULL)
RETURNS int[] AS $$

--DEFINE GENERAL VARIABLES
DECLARE mov_date date;DECLARE mov_time varchar;DECLARE sit_date date;
DECLARE mp_id integer;DECLARE mp_coupon_id integer;DECLARE num_stairs integer;
DECLARE total_cubic_feet numeric;DECLARE item_cubic_feet numeric;
DECLARE num_carpentry integer;DECLARE num_crating integer;
DECLARE box_dow integer;DECLARE box_date date;DECLARE box_cubic_feet numeric;
DECLARE frozen_pc_id integer; DECLARE frozen_mover_id integer;
DECLARE frozen_mover_latest_pc_id integer; DECLARE white_label_movers int[];

--DEFINE VARIABLES: PICKUP(pu_), EXTRA PICK UP(epu_), DROP OFF(do_), EXTRA DROP OFF(edo_)
DECLARE pu_state varchar; DECLARE pu_earth earth; DECLARE pu_key varchar;
DECLARE epu_state varchar;DECLARE epu_earth earth;DECLARE epu_key varchar;
DECLARE do_state varchar; DECLARE do_earth earth; DECLARE do_key varchar;
DECLARE edo_state varchar;DECLARE edo_earth earth;DECLARE edo_key varchar;

DECLARE
  BEGIN
    --SET GENERAL VARIABLES
    mp_id := (SELECT uuidable_id FROM uuids WHERE uuids.uuid = $1 AND uuidable_type = 'MovePlan');
    DROP TABLE IF EXISTS mp;
    CREATE TEMP TABLE mp AS (SELECT * FROM move_plans WHERE move_plans.id = mp_id);
    frozen_pc_id := COALESCE((SELECT jobs.price_chart_id FROM jobs WHERE mover_state <> 'declined' AND user_state NOT in('reserved_cancelled', 'cancelled') AND move_plan_id = mp_id LIMIT 1),(SELECT frozen_price_chart_id FROM mp));
    frozen_mover_id := (SELECT price_charts.mover_id FROM price_charts WHERE price_charts.id = frozen_pc_id);
    frozen_mover_latest_pc_id := (SELECT price_charts.id FROM price_charts WHERE price_charts.mover_id = frozen_mover_id ORDER BY created_at DESC LIMIT 1);
    white_label_movers := (SELECT array_agg(mover_id) FROM white_label_whitelists WHERE white_label_id = (SELECT white_label_id FROM mp));
    mov_date := (SELECT move_date FROM mp);
    mov_time := (SELECT CASE WHEN mp.move_time LIKE '%PM%' THEN 'pm' ELSE 'am' END FROM mp );
    sit_date := (SELECT storage_move_out_date FROM mp);
    box_date := (SELECT box_delivery_date FROM mp);
    box_dow := (SELECT EXTRACT(isodow FROM box_date :: DATE));
    num_stairs := (
      SELECT sum(flights_of_stairs) FROM (
        SELECT
          heights.flights_of_stairs
        FROM addresses
        JOIN heights
          ON addresses.height_id = heights.id
          AND addresses.move_plan_id = mp_id
          AND addresses.role_in_plan IN ('drop_off', 'pick_up') ) as stairs);
    mp_coupon_id := (
      SELECT COALESCE(
          (SELECT coupon_id FROM jobs WHERE jobs.move_plan_id = mp_id AND user_state <> 'cancelled' AND mover_state <> 'declined' ORDER BY jobs.id LIMIT 1),
          (SELECT coupon_id FROM jobs WHERE jobs.move_plan_id = mp_id AND user_state = 'cancelled' ORDER BY jobs.id DESC LIMIT 1)));

    --FIND LIVE MOVERS
    DROP TABLE IF EXISTS potential_movers;
    CREATE TEMP TABLE potential_movers AS SELECT
        branch_properties.name as mover_name,
        movers.id,
        CASE WHEN frozen_mover_id = movers.id THEN
          frozen_pc_id
        ELSE
          latest_pc.latest_pc_id
        END as latest_pc_id,
        movers.local_consult_only,
        movers.interstate_consult_only
      FROM movers
      JOIN branch_properties
        ON branchable_id = movers.id
        AND branchable_type = 'Mover'
        AND marketplace_status = 'live'
        AND (is_hidden = false OR movers.id = any(white_label_movers))
        AND (CASE WHEN mover_param IS NOT NULL THEN
             movers.id = any(mover_param)
            ELSE 1=1 END)
	      JOIN (SELECT
	              id as latest_pc_id,
	              price_charts.mover_id AS pc_mover_id,
	              rank() OVER(
	                PARTITION BY price_charts.mover_id
	                ORDER BY created_at DESC)
	            FROM public.price_charts) as latest_pc
	        ON pc_mover_id = movers.id
	        AND ((RANK = 1 AND latest_pc.pc_mover_id <> frozen_mover_id) OR latest_pc.latest_pc_id = frozen_pc_id);
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
            AND (price_charts.max_cubic_feet IS NULL OR total_cubic_feet <= price_charts.max_cubic_feet)
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
            AND (price_charts.max_cubic_feet IS NULL OR total_cubic_feet <= price_charts.max_cubic_feet)
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
        WHERE CASE WHEN mov_time = 'am' THEN availability.net_am > 0
        ELSE availability.net_pm > 0
        END
    );

        --RAISE NO MOVER FOUND ERROR
        IF (SELECT COUNT(*) FROM movers_with_location_and_balancing_rate) = 0 THEN
          RAISE EXCEPTION 'No movers are available on this move date';
        END IF;

      --FILTER BY SIT AVAILABILITY
    IF sit_date IS NOT NULL THEN
      DELETE FROM movers_with_location_and_balancing_rate WHERE movers_with_location_and_balancing_rate.sit_avail <= 0;
    END IF;

RETURN (SELECT array_agg(movers_with_location_and_balancing_rate.mover_id) FROM movers_with_location_and_balancing_rate);

END; $$
LANGUAGE plpgsql;