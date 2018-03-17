SELECT * FROM filtered_movers_with_pricing('e00bf78a-ff94-11e7-7aad-0f461a27ccab');
SELECT * FROM distance_in_miles('"65658 Broadway", New York, NY, 10012','11377');
SELECT * FROM comparison_presenter_v4('3942a54a-0b9f-11e8-b1b5-0f461a27ccab');

DROP FUNCTION IF EXISTS distance_in_miles(VARCHAR, VARCHAR);
CREATE FUNCTION distance_in_miles(addr1 VARCHAR, addr2 VARCHAR)
  RETURNS numeric AS
$func$
BEGIN
RETURN (SELECT distance_in_miles
        FROM driving_distances
        WHERE key = (
          SELECT string_agg(key, '::' order by key ASC)
          FROM (
                 SELECT
                   CAST(addr1 AS varchar) AS key,
                   1 AS id
                 UNION (SELECT
                   CAST(addr2 AS varchar) as key,
                   1 as id
                 )) AS keys
          GROUP BY id)
        OR key = (
          SELECT string_agg(key, '::' order by key DESC)
          FROM (
                 SELECT
                   CAST(addr1 AS varchar) AS key,
                   1 AS id
                 UNION (SELECT
                   CAST(addr2 AS varchar) as key,
                   1 as id
                 )) AS keys
          GROUP BY id)
        LIMIT 1);
END
$func$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS filtered_movers_with_pricing(move_plan_param VARCHAR);
CREATE FUNCTION filtered_movers_with_pricing(move_plan_param VARCHAR)
RETURNS TABLE(
  total numeric,
  mover_special_discount numeric, facebook_discount numeric,
  twitter_discount numeric, coupon_discount numeric,
  subtotal numeric,
  moving_cost_adjusted numeric, ravel_cost_adjusted numeric,
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
  balancing_rate_secondary NUMERIC, net_am BIGINT, net_pm BIGINT) AS $$

--DEFINE GENERAL VARIABLES
DECLARE mov_date date;DECLARE mov_time varchar;DECLARE num_stairs integer;
DECLARE mp_id integer;DECLARE item_cubic_feet numeric;DECLARE box_cubic_feet numeric;
DECLARE total_cubic_feet numeric;DECLARE num_carpentry integer;DECLARE num_crating integer;
DECLARE box_dow integer;DECLARE box_date date;DECLARE mp_coupon_id integer;

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
    mov_date := (SELECT move_date FROM mp);
    mov_time :=(SELECT CASE WHEN mp.move_time LIKE '%PM%' THEN 'pm' ELSE 'am' END FROM mp );
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
        latest_pc.latest_pc_id as latest_pc_id,
        movers.local_consult_only,
        movers.interstate_consult_only
      FROM movers
      JOIN branch_properties
        ON branchable_id = movers.id
        AND branchable_type = 'Mover'
        AND state = 'live'
        AND is_hidden ='false'
      JOIN (SELECT
              id as latest_pc_id,
              price_charts.mover_id AS pc_mover_id,
              rank() OVER(
                PARTITION BY price_charts.mover_id
                ORDER BY created_at DESC)
            FROM public.price_charts) as latest_pc
        ON pc_mover_id = movers.id
        AND RANK = 1;

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

    --FIND CRATING ITEMS CUBIC FEET
    DROP TABLE IF EXISTS crating_item_cubic_feet;
    CREATE TEMP TABLE crating_item_cubic_feet AS(
      SELECT cubic_feet
      FROM mp_ii WHERE crating_required = true
    );

    --FIND TOTAL ITEM CUBIC FEET
    item_cubic_feet := (SELECT SUM(cubic_feet) FROM mp_ii);

    --FIND TOTAL BOX CUBIC FEET
    box_cubic_feet := (
      SELECT SUM(cubic_feet * quantity)
      FROM mp_bi
      GROUP BY mp_bi.move_plan_id);

    --FIND TOTAL CUBIC FEET
    total_cubic_feet := (SELECT box_cubic_feet + item_cubic_feet);

    --FIND NUMBER OF CRATING ITEMS
    num_crating := (SELECT count(*) FROM crating_item_cubic_feet);

    --FIND NUMBER OF 'CARPENTRY' ITEMS
    num_carpentry := (SELECT count(*) FROM mp_ii WHERE wall_removal_required = true OR assembly_required = true);

    --SET ADDRESS VARIABLES USING THIS STUPID METHOD BECAUSE THE RAILS DEVELOPERS CAN'T READ THE POSTGRES DOCUMENTATION
    --https://www.postgresql.org/docs/current/static/sql-select.html Description #8
    DROP TABLE IF EXISTS mp_addresses;
    CREATE TEMP TABLE mp_addresses AS
      (SELECT * FROM addresses WHERE move_plan_id = mp_id AND geocoded_address IS NOT NULL AND role_in_plan = 'pick_up' LIMIT 1) UNION
      (SELECT * FROM addresses WHERE move_plan_id = mp_id AND geocoded_address IS NOT NULL AND role_in_plan = 'extra_pick_up' LIMIT 1) UNION
      (SELECT * FROM addresses WHERE move_plan_id = mp_id AND geocoded_address IS NOT NULL AND role_in_plan = 'drop_off' LIMIT 1) UNION
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
    pu_key :=   (SELECT
                CASE WHEN (street_address is null OR street_address = '') AND zip IS NOT NULL THEN
                  city || ', ' || state || ', '|| zip
                WHEN street_address is not null AND zip IS NULL THEN
                  '"' || street_address ||'"'|| ', ' || city ||', '|| state
                WHEN (street_address is null OR street_address = '') AND zip IS NULL THEN
                  city || ', ' || state
                ELSE
                  '"' || street_address ||'"'|| ', ' || city ||', '|| state || ', '|| zip
                END  FROM mp_addresses WHERE role_in_plan = 'pick_up');
    epu_key :=  (SELECT
                CASE WHEN (street_address is null OR street_address = '') AND zip IS NOT NULL THEN
                  city || ', ' || state || ', '|| zip
                WHEN street_address is not null AND zip IS NULL THEN
                  '"' || street_address ||'"'|| ', ' || city ||', '|| state
                WHEN (street_address is null OR street_address = '') AND zip IS NULL THEN
                  city || ', ' || state
                ELSE
                  '"' || street_address ||'"'|| ', ' || city ||', '|| state || ', '|| zip
                END  FROM mp_addresses WHERE role_in_plan = 'extra_pick_up');
    do_key :=   (SELECT
                CASE WHEN (street_address is null OR street_address = '') AND zip IS NOT NULL THEN
                  city || ', ' || state || ', '|| zip
                WHEN street_address is not null AND zip IS NULL THEN
                  '"' || street_address ||'"'|| ', ' || city ||', '|| state
                WHEN (street_address is null OR street_address = '') AND zip IS NULL THEN
                  city || ', ' || state
                ELSE
                  '"' || street_address ||'"'|| ', ' || city ||', '|| state || ', '|| zip
                END  FROM mp_addresses WHERE role_in_plan = 'drop_off');
    edo_key := (SELECT
                CASE WHEN (street_address is null OR street_address = '') AND zip IS NOT NULL THEN
                  city || ', ' || state || ', '|| zip
                WHEN street_address is not null AND zip IS NULL THEN
                  '"' || street_address ||'"'|| ', ' || city ||', '|| state
                WHEN (street_address is null OR street_address = '') AND zip IS NULL THEN
                  city || ', ' || state
                ELSE
                  '"' || street_address ||'"'|| ', ' || city ||', '|| state || ', '|| zip
                END  FROM mp_addresses WHERE role_in_plan = 'extra_drop_off');

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
            AND (price_charts.max_cubic_feet IS NULL OR item_cubic_feet <= price_charts.max_cubic_feet)
          JOIN additional_services
            ON additional_services.price_chart_id = price_charts.id
          JOIN storage_details
            ON storage_details.additional_services_id = additional_services.id;

        --RAISE NO MOVER FOUND ERROR
        IF (SELECT COUNT(*) FROM movers_by_haul) = 0 THEN
          RAISE EXCEPTION 'No interstate movers support this pick up address or can carry such a large amount';
        END IF;

    --INTRASTATE MOVES
    ELSE
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
            IN (SELECT * FROM movers_by_haul
              JOIN price_charts
              ON price_charts.id = movers_by_haul.latest_pc_id
              AND (price_charts.minimum_job_distance * 1609.34) >= (SELECT * FROM earth_distance(COALESCE(do_earth,mover_earth),pu_earth)));
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
      WHERE (SELECT * FROM earth_distance(movers_by_haul.mover_earth, COALESCE(do_earth,pu_earth))) <= movers_by_haul.drop_off_mileage * 1609.34);

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
      FROM (SELECT mover_locations.*, earth_distance(ll_to_earth(latitude,longitude),do_earth)/1609.34 AS distance_in_miles, rank()
        OVER (PARTITION BY mover_locations.price_chart_id ORDER BY earth_distance(ll_to_earth(latitude,longitude),do_earth) ASC, mover_locations.cents_per_cubic_foot DESC)
          FROM public.mover_locations
            WHERE state_code IN (do_state, edo_state)
            AND mover_locations.active = TRUE
            AND mover_locations.location_type='city'
            AND mover_locations.price_chart_id IN (SELECT DISTINCT mover_state_locations.price_chart_id FROM mover_state_locations WHERE mover_state_locations.partially_active = true)
            AND mover_locations.price_chart_id NOT IN (SELECT DISTINCT mover_full_state_locations.price_chart_id FROM mover_full_state_locations)) AS closest_locations
      WHERE RANK = 1 AND closest_locations.distance_in_miles <= closest_locations.range);

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
      DELETE FROM movers_with_location WHERE movers_with_location.warehousing = false;
    END IF;

        --RAISE NO MOVER FOUND ERROR
        IF (SELECT COUNT(*) FROM movers_with_location) = 0 THEN
          RAISE EXCEPTION 'No movers can support moving into storage';
        END IF;

    --FILTER BY SIT
    IF (SELECT storage_move_out_date FROM mp) IS NOT NULL THEN
      DELETE FROM movers_with_location WHERE movers_with_location.storage_in_transit = false;
    END IF;

        --RAISE NO MOVER FOUND ERROR
        IF (SELECT COUNT(*) FROM movers_with_location) = 0 THEN
          RAISE EXCEPTION 'No movers can support storage in transit';
        END IF;

    --FILTER BY PHONE REQUEST
    IF (SELECT count(*) FROM onsite_requests WHERE move_plan_id = mp_id AND type = 'InHomeRequest') > 0 THEN
      DELETE FROM movers_with_location WHERE movers_with_location.onsites = false;
    END IF;

        --RAISE NO MOVER FOUND ERROR
        IF (SELECT COUNT(*) FROM movers_with_location) = 0 THEN
          RAISE EXCEPTION 'No movers can support phone requests';
        END IF;

    --FILTER BY ONSITE REQUEST
    IF (SELECT count(*) FROM onsite_requests WHERE move_plan_id = mp_id AND type = 'PhoneRequest') > 0 THEN
      DELETE FROM movers_with_location WHERE movers_with_location.callback = false;
    END IF;

        --RAISE NO MOVER FOUND ERROR
        IF (SELECT COUNT(*) FROM movers_with_location) = 0 THEN
          RAISE EXCEPTION 'No movers can support onsite requests';
        END IF;

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
      SELECT * FROM (
        SELECT
          mwl.*,

          --BALANCING RATE
          COALESCE(daily.balancing_rate_primary, weekly.balancing_rate_primary) AS balancing_rate_primary,
          COALESCE(daily.balancing_rate_secondary, weekly.balancing_rate_secondary) AS balancing_rate_secondary,

          --AVAILABILITY CALCULATIONS
          CASE WHEN COALESCE(daily.capacity_secondary, weekly.capacity_secondary) IS NULL
            THEN COALESCE(daily.capacity_primary, weekly.capacity_primary) - COALESCE(am, 0) - COALESCE(pm, 0)
          ELSE COALESCE(daily.capacity_secondary, weekly.capacity_secondary) - COALESCE(pm, 0)
            END as net_pm,
          CASE WHEN COALESCE(daily.capacity_secondary, weekly.capacity_secondary) IS NULL
            THEN COALESCE(daily.capacity_primary, weekly.capacity_primary) - COALESCE(am, 0) - COALESCE(pm, 0)
          ELSE COALESCE(daily.capacity_primary, weekly.capacity_primary) - COALESCE(am, 0)
            END as net_am
        FROM movers_with_location as mwl

        --ADJUSTMENTS BY DATE
        LEFT JOIN(
           SELECT *
           FROM PUBLIC.daily_adjustments AS day_adj
           JOIN PUBLIC.daily_adjustment_data AS adj_data
             ON day_adj.daily_adjustment_datum_id = adj_data.id) AS daily
         ON mov_date  = day
          AND mwl.price_chart_id = daily.price_chart_id

        --ADJUSTMENTS BY WEEKDAY
        LEFT JOIN(
           SELECT *
           FROM PUBLIC.daily_adjustment_rules AS rul_adj
           JOIN PUBLIC.daily_adjustment_data AS adj_data
             ON rul_adj.daily_adjustment_datum_id = adj_data.id) AS weekly
        ON weekday =
           CASE WHEN EXTRACT(isodow FROM mov_date :: DATE) = 7 THEN
              0
            ELSE
              EXTRACT(isodow FROM mov_date :: DATE)
            END
        AND mwl.price_chart_id = weekly.price_chart_id

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
        AND mov_date = jobs.move_date) AS availability
      WHERE CASE WHEN mov_time = 'am' THEN availability.net_am > 0
        ELSE availability.net_pm > 0
        END
    );

        --RAISE NO MOVER FOUND ERROR
        IF (SELECT COUNT(*) FROM movers_with_location_and_balancing_rate) = 0 THEN
          RAISE EXCEPTION 'No movers are available on this move date';
        END IF;


    --PRECOMPUTE TRAVEL PLAN
    DROP TABLE IF EXISTS travel_plan_miles;
    CREATE TEMP TABLE travel_plan_miles AS (SELECT
        mwlabr.latest_pc_id,

        --WAREHOUSE TO PICK UP DISTANCE
      ((SELECT * FROM distance_in_miles(pu_key,price_charts.zip)) +

          --HANDLE LOCAL
        (CASE WHEN mwlabr.location_type = 'local' AND do_state IS NOT NULL AND (SELECT storage_move_out_date FROM mp) IS NULL THEN

            --HANDLE EXTRA PICK UP
          (CASE WHEN epu_state IS NOT NULL THEN

              --PICK UP TO EXTRA PICK UP DISTANCE
            (SELECT * FROM distance_in_miles(pu_key,epu_key)) +

              --EXTRA PICK UP TO DROP OFF DISTANCE
            (SELECT * FROM distance_in_miles(epu_key,do_key))
          ELSE

              --PICK UP TO DROP OFF DISTANCE
            (SELECT * FROM distance_in_miles(pu_key,do_key))
          END) +

            --HANDLE EXTRA DROP OFF
          (CASE WHEN edo_state IS NOT NULL THEN

              --DROP OFF TO EXTRA DROP OFF DISTANCE
            (SELECT * FROM distance_in_miles(do_key,edo_key)) +

              --EXTRA DROP OFF TO WAREHOUSE DISTANCE
            (SELECT * FROM distance_in_miles(edo_key,price_charts.zip))
          ELSE

              --DROP OFF TO WAREHOUSE DISTANCE
            (SELECT * FROM distance_in_miles(do_key,price_charts.zip))
          END)

          --HANDLE LOCAL SIT
        WHEN mwlabr.location_type = 'local' AND do_state IS NOT NULL AND (SELECT storage_move_out_date FROM mp) IS NOT NULL THEN

            --HANDLE EXTRA PICK UP
          (CASE WHEN epu_state IS NOT NULL THEN

              --PICK UP TO EXTRA PICK UP DISTANCE
            (SELECT * FROM distance_in_miles(pu_key,epu_key))+

              --EXTRA PICK UP TO WAREHOUSE DISTANCE
            (SELECT * FROM distance_in_miles(epu_key,price_charts.zip))
          ELSE

              --PICK UP TO WAREHOUSE DISTANCE
            (SELECT * FROM distance_in_miles(pu_key,price_charts.zip))
          END) +

            --HANDLE EXTRA DROP OFF
          (CASE WHEN edo_state IS NOT NULL THEN

              --WAREHOUSE TO DROP OFF DISTANCE
            (SELECT * FROM distance_in_miles(price_charts.zip,do_key)) +

              --DROP OFF TO EXTRA DROP OFF DISTANCE
            (SELECT * FROM distance_in_miles(do_key,edo_key)) +

              --EXTRA DROP OFF TO WAREHOUSE DISTANCE
            (SELECT * FROM distance_in_miles(edo_key,price_charts.zip))
          ELSE

              --DROP OFF TO WAREHOUSE DISTANCE
            (SELECT * FROM distance_in_miles(do_key,price_charts.zip)) * 2
          END)

          --SUBTRACT EXTRA FREE MILES FOR LOCAL SIT
          - price_charts.free_miles

          --HANDLE LONG DISTANCE AND MOVE INTO STORAGE
        ELSE
          (CASE WHEN epu_state IS NOT NULL THEN

              --PICK UP TO EXTRA PICK UP DISTANCE
            (SELECT * FROM distance_in_miles(pu_key,epu_key))+

              --EXTRA PICK UP TO WAREHOUSE DISTANCE
            (SELECT * FROM distance_in_miles(epu_key,price_charts.zip))
          ELSE

              --PICK UP TO WAREHOUSE DISTANCE
            (SELECT * FROM distance_in_miles(pu_key,price_charts.zip))
          END)
        END)

        --SUBTRACT FREE MILES FOR ALL MOVES
        - price_charts.free_miles) AS distance_minus_free
       FROM movers_with_location_and_balancing_rate AS mwlabr
        JOIN price_charts
        ON mwlabr.latest_pc_id = price_charts.id
    );

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

    --CARDBOARD/PACKING/UNPACKING COST BY PRICE_CHART
    DROP TABLE IF EXISTS cb_p_up_cost_pc;
    CREATE TEMP TABLE cb_p_up_cost_pc AS (SELECT

      --HANDLE BOX DELIVERY
      CASE WHEN (SELECT mp.box_delivery_date FROM MP) IS NOT NULL THEN
        SUM(cents_for_cardboard/100.00 * CAST(quantity AS NUMERIC)) +

        --FIGURE OUT RIDICULOUS BOX_DELIVERY_FEE (WHAT ARE THESE PATTERNS???????)
        (CASE box_dow
          WHEN 1 THEN cb_p_up_pc.box_delivery_fee_monday
          WHEN 2 THEN cb_p_up_pc.box_delivery_fee_thursday
          WHEN 3 THEN cb_p_up_pc.box_delivery_fee_wednesday
          WHEN 4 THEN cb_p_up_pc.box_delivery_fee_thursday
          WHEN 5 THEN cb_p_up_pc.box_delivery_fee_friday
          WHEN 6 THEN cb_p_up_pc.box_delivery_fee_saturday
          WHEN 7 THEN cb_p_up_pc.box_delivery_fee_sunday
        ELSE 0.00 END)
      ELSE 0.00
      END AS cardboard_cost,

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

      --BALANCING RATE FOR BOX DELIVERY (THIS TRIGGERS ME)
      COALESCE(daily.balancing_rate_primary, weekly.balancing_rate_primary) AS box_balancing_rate_primary,
      COALESCE(daily.balancing_rate_secondary, weekly.balancing_rate_secondary) AS box_balancing_rate_secondary,
      cb_p_up_pc.id AS cb_p_up_pc_id
    FROM mp_bi
    JOIN price_charts AS cb_p_up_pc
    ON cb_p_up_pc.id IN (SELECT cb_p_up_mwlabr.latest_pc_id FROM movers_with_location_and_balancing_rate AS cb_p_up_mwlabr)
    JOIN box_type_rates AS btr
    ON cb_p_up_pc.id = btr.price_chart_id AND btr.box_type_id = mp_bi.box_type_id

    --BOX DELIVERY ADJUSTMENTS BY DATE
    LEFT JOIN(
       SELECT *
       FROM PUBLIC.daily_adjustments AS day_adj
       JOIN PUBLIC.daily_adjustment_data AS adj_data
         ON day_adj.daily_adjustment_datum_id = adj_data.id) AS daily
    ON box_date  = day
      AND cb_p_up_pc.id  = daily.price_chart_id

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
    AND cb_p_up_pc.id  = weekly.price_chart_id
    GROUP BY
      cb_p_up_pc.id,
      cb_p_up_pc.packing_flat_fee,
      COALESCE(daily.balancing_rate_primary, weekly.balancing_rate_primary),
      COALESCE(daily.balancing_rate_secondary, weekly.balancing_rate_secondary));

    --GET MOVER SPECIAL DISCOUNTS
    DROP TABLE IF EXISTS mover_special_pc;
    CREATE TEMP TABLE mover_special_pc AS (
    SELECT *
    FROM mover_specials AS ms
    WHERE ms.price_chart_id IN (SELECT ms_mwlabr.latest_pc_id FROM movers_with_location_and_balancing_rate AS ms_mwlabr)
    AND active = true
    );

--DO ALL THE PRICING STUFF (oh boi)
DROP TABLE IF EXISTS movers_and_pricing;
CREATE TEMP TABLE movers_and_pricing AS (
  SELECT

  --TOTAL
    total.subtotal + total.mover_special_discount + total.facebook_discount + total.twitter_discount + total.coupon_discount AS total,
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

      --FACEBOOK DISCOUNT
      CASE WHEN (SELECT mp.shared_on_facebook = true FROM mp) THEN
          -5.00
      ELSE
          0
      END AS facebook_discount,

      --TWITTER DISCOUNT
      CASE WHEN (SELECT mp.shared_on_twitter = true FROM mp) THEN
          -5.00
      ELSE
          0
      END AS twitter_discount,

      --COUPON DISCOUNT
      CASE
      WHEN COALESCE((SELECT percentage FROM coupons WHERE mp_coupon_id = coupons.id AND active = TRUE ), false) = true THEN
          (SELECT discount_percentage FROM coupons WHERE mp_coupon_id = coupons.id AND active = TRUE ) *
          -1.00 / 100.00 *
          subtotal.subtotal
      WHEN COALESCE((SELECT percentage FROM coupons WHERE mp_coupon_id = coupons.id AND active = TRUE ), true) = false THEN
          (SELECT discount_cents FROM coupons WHERE mp_coupon_id = coupons.id AND active = TRUE ) *
          -1.00 / 100.00
      ELSE
          0
      END AS coupon_discount,
      subtotal.*
    FROM(
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
            ((CASE WHEN mwlabr.location_type = 'local' AND (SELECT storage_move_out_date FROM mp) IS NOT NULL  THEN
              Cast(price_charts.cents_per_truck / 100.00 * 2.00 as numeric)
            ELSE
              Cast(price_charts.cents_per_truck / 100.00 as numeric)
            END) +

            --DISTANCE COST ADJUSTED
            (CASE WHEN travel_plan_miles.distance_minus_free < 0 THEN
              0.00
            ELSE
              travel_plan_miles.distance_minus_free * price_charts.cents_per_mile / 100.00
            END)
            +

            --HANDLE EXTRA LONG DISTANCE COSTS
            (CASE WHEN mwlabr.location_type = 'local' THEN
              0.00
            ELSE

              --LONG DISTANCE CUBIC FEET COST WITH PRICING TIERS
              ((total_cubic_feet * mwlabr.cents_per_cubic_foot / 100.00 * COALESCE(long_distance_tiers_coefficient,1.00) ) + COALESCE(mwlabr.extra_fee,0.00))
              +

              --EXTRA DROP OFF LOCAL COST
              (CASE WHEN edo_state IS NULL THEN
                0.00
              ELSE
                (SELECT * FROM distance_in_miles(do_key,edo_key)) * price_charts.cents_per_mile / 100.00
              END)
            END)) *

            --MULTIPLY ABOVE BY BALANCING RATE
            balancing_rate.rate,
          2) AS travel_cost_adjusted,

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
            (CASE WHEN do_state IS NULL AND (SELECT storage_move_out_date FROM mp) IS NOT NULL THEN
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
          ROUND((((cb_p_up_cost_pc.packing_cost) +
                  (cb_p_up_cost_pc.unpacking_cost) +
                  price_charts.packing_flat_fee) *
                  balancing_rate.rate),2) AS packing_cost_adjusted,

        --CARDBOARD COST ADJUSTED
          ROUND(((cb_p_up_cost_pc.cardboard_cost) *

            --BALANCING RATE ON BOX DELIVERY DAY (ICKY)
           (1.00 + (
            (CASE WHEN mov_time = 'am' THEN
              coalesce(cb_p_up_cost_pc.box_balancing_rate_primary,0.00)
              ELSE
              coalesce(cb_p_up_cost_pc.box_balancing_rate_secondary, cb_p_up_cost_pc.box_balancing_rate_primary,0.00)
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

        --JOIN PRECOMPUTED SPECIAL HANDLING COSTS
        LEFT JOIN cb_p_up_cost_pc
          ON cb_p_up_cost_pc.cb_p_up_pc_id = mwlabr.latest_pc_id

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
  ) AS subtotal
) AS total);
RETURN QUERY SELECT * FROM movers_and_pricing ORDER BY (
  (movers_and_pricing.local_consult_only OR movers_and_pricing.interstate_consult_only),movers_and_pricing.total
) ASC;
END; $$
LANGUAGE plpgsql;


DROP FUNCTION IF EXISTS comparison_presenter_v4(VARCHAR);
CREATE FUNCTION comparison_presenter_v4(move_plan_param VARCHAR)
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
RETURN QUERY (SELECT
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
        bp.name,
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
        FROM filtered_movers_with_pricing(move_plan_param) AS pricing
        JOIN movers ON movers.id = pricing.mover_id
        JOIN branch_properties AS bp ON bp.branchable_id = movers.id AND branchable_type = 'Mover'
        JOIN base_addresses AS ba ON bp.id = ba.branch_property_id
        JOIN service_provider_ratings AS yelp ON yelp.service_provider_id = movers.id AND yelp.service_provider_type = 'Mover'AND yelp.reviewer = 'Yelp'
        JOIN service_provider_ratings AS google ON google.service_provider_id = movers.id AND google.service_provider_type = 'Mover'AND google.reviewer = 'Google'
        JOIN service_provider_ratings AS unpakt ON unpakt.service_provider_id = movers.id AND unpakt.service_provider_type = 'Mover'AND unpakt.reviewer = 'Unpakt'
       );
END
$func$ LANGUAGE plpgsql;