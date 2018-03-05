SELECT * FROM filtered_movers_with_pricing('ac5e51bc-1e34-11e8-9cb4-c76ccf169b86');
SELECT * FROM distance_in_miles('"65658 Broadway", New York, NY, 10012','11377');

DROP FUNCTION IF EXISTS distance_in_miles(VARCHAR, VARCHAR);
CREATE FUNCTION distance_in_miles(addr1 VARCHAR, addr2 VARCHAR)
  RETURNS numeric AS
$func$
BEGIN
RETURN (SELECT distance_in_miles
        FROM driving_distances
        WHERE key = (
          SELECT string_agg(key, '::' order by key)
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
  moving_cost_adjusted numeric,
  travel_cost_adjusted numeric,
  special_handling_cost_adjusted numeric,
  storage_cost numeric,
  packing_cost_adjusted numeric,
  cardboard_cost_adjusted numeric,
  mover_name varchar, mover_id integer,
  pick_up_mileage numeric, drop_off_mileage numeric,
  extra_stop_enabled boolean,
  packing boolean,unpacking boolean, box_delivery boolean,
  piano boolean, storage boolean, onsites boolean,callback boolean,
  crating boolean,disassembly_assembly boolean,
  wall_dismounting boolean, box_delivery_range numeric,
  storage_in_transit boolean, warehousing boolean,
  local_cents_per_cubic_foot numeric, pu_lat numeric,
  pu_long numeric, latest_pc_id integer,
  mover_earth earth, mover_location_id integer,
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
DECLARE box_delivery_dow integer;
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
    box_delivery_dow :=  (SELECT EXTRACT(isodow FROM mov_date :: DATE));
    num_stairs := (
      SELECT sum(flights_of_stairs) FROM (
        SELECT
          heights.flights_of_stairs
        FROM addresses
        JOIN heights
          ON addresses.height_id = heights.id
          AND addresses.move_plan_id = mp_id
          AND addresses.role_in_plan IN ('drop_off', 'pick_up') ) as stairs);
    --FIND LIVE MOVERS
    DROP TABLE IF EXISTS potential_movers;
    CREATE TEMP TABLE potential_movers AS SELECT
        branch_properties.name as mover_name, movers.id, latest_pc.latest_pc_id as latest_pc_id
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
    DROP TABLE IF EXISTS crating_item_cubic_feet;
    CREATE TEMP TABLE crating_item_cubic_feet AS(
      SELECT cubic_feet
      FROM mp_ii WHERE crating_required = true
    );
    item_cubic_feet := (SELECT SUM(cubic_feet) FROM mp_ii);
    box_cubic_feet := (
      SELECT SUM(cubic_feet * quantity)
      FROM mp_bi
      GROUP BY mp_bi.move_plan_id);
    total_cubic_feet := (SELECT box_cubic_feet + item_cubic_feet);
    num_crating := (SELECT count(*) FROM crating_item_cubic_feet);
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
                CASE WHEN street_address is null OR street_address = '' AND zip IS NOT NULL THEN
                  city || ', ' || state || ', '|| zip
                WHEN street_address is null OR street_address = '' AND zip IS NULL THEN
                  city || ', ' || state
                ELSE
                  '"' || street_address ||'"'|| ', ' || city ||', '|| state || ', '|| zip
                END  FROM mp_addresses WHERE role_in_plan = 'pick_up');
    epu_key :=  (SELECT
                CASE WHEN street_address is null OR street_address = '' AND zip IS NOT NULL THEN
                  city || ', ' || state || ', '|| zip
                WHEN street_address is null OR street_address = '' AND zip IS NULL THEN
                  city || ', ' || state
                ELSE
                  '"' || street_address ||'"'|| ', ' || city ||', '|| state || ', '|| zip
                END  FROM mp_addresses WHERE role_in_plan = 'extra_pick_up');
    do_key :=   (SELECT
                CASE WHEN street_address is null OR street_address = '' AND zip IS NOT NULL THEN
                  city || ', ' || state || ', '|| zip
                WHEN street_address is null OR street_address = '' AND zip IS NULL THEN
                  city || ', ' || state
                ELSE
                  '"' || street_address ||'"'|| ', ' || city ||', '|| state || ', '|| zip
                END  FROM mp_addresses WHERE role_in_plan = 'drop_off');
    edo_key := (SELECT
                CASE WHEN street_address is null OR street_address = '' AND zip IS NOT NULL THEN
                  city || ', ' || state || ', '|| zip
                WHEN street_address is null OR street_address = '' AND zip IS NULL THEN
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
      pu_lat numeric, pu_long numeric, latest_pc_id integer, mover_earth earth);
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
          (SELECT * FROM ll_to_earth(price_charts.latitude, price_charts.longitude)) AS mover_earth
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
          (SELECT * FROM ll_to_earth(price_charts.latitude, price_charts.longitude)) AS mover_earth
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
            --CHECK FOR MINIMUM DISTANCE IN PA AND IL
          IF pu_state in ('IL', 'PA') THEN
            DELETE FROM movers_by_haul
            WHERE movers_by_haul.mover_id
            IN (SELECT * FROM movers_by_haul
              JOIN price_charts
              ON price_charts.id = movers_by_haul.latest_pc_id
              AND (price_charts.minimum_job_distance * 1609.34) >= (SELECT * FROM earth_distance(COALESCE(do_earth,mover_earth),pu_earth)));
          END IF;
    END IF;
    --FILTER BY EXTRA PICK UP
    IF (SELECT extra_pick_up_enabled FROM mp) = true THEN
      DELETE FROM movers_by_haul WHERE (SELECT * FROM earth_distance(epu_earth,movers_by_haul.mover_earth)) > pick_up_mileage * 1609.34;
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
    --FILTER BY EXTRA DROP OFF
    IF (SELECT mp.extra_drop_off_enabled FROM mp) = true THEN
      DELETE FROM movers_with_location WHERE movers_with_location.extra_stop_enabled = false;
      DELETE FROM movers_with_location WHERE movers_with_location.location_type = 'local' AND earth_distance(movers_with_location.mover_earth,edo_earth)/1609.34 > movers_with_location.drop_off_mileage;
      DELETE FROM movers_with_location WHERE movers_with_location.location_type = 'state' AND earth_distance(do_earth,edo_earth)/1609.34 > GREATEST(50.0,movers_with_location.drop_off_mileage);
      DELETE FROM movers_with_location WHERE movers_with_location.location_type = 'city'  AND earth_distance(ll_to_earth(movers_with_location.location_latitude,movers_with_location.location_longitude),edo_earth)/1609.34 > movers_with_location.range;
    END IF;
    --FILTERING BY PACKING SERVICE
    IF (SELECT follow_up_packing_service_id FROM mp) = 1 OR (SELECT initial_packing_service_id FROM mp) = 1 THEN
      DELETE FROM movers_with_location WHERE movers_with_location.packing = false;
    END IF;
    IF (SELECT follow_up_packing_service_id FROM mp) = 2 OR (SELECT initial_packing_service_id FROM mp) = 2 THEN
      DELETE FROM movers_with_location WHERE movers_with_location.packing = false;
      DELETE FROM movers_with_location WHERE movers_with_location.unpacking = false;
    END IF;
    --FILTER BY BOX DELIVERY
    IF (SELECT box_delivery_date FROM mp) IS NOT NULL THEN
      DELETE FROM movers_with_location WHERE movers_with_location.box_delivery = false;
      DELETE FROM movers_with_location WHERE earth_distance(pu_earth,movers_with_location.mover_earth) > movers_with_location.box_delivery_range;
    END IF;
    --FILTER BY PIANO
    IF (SELECT COUNT(*) FROM mp_ii WHERE requires_piano_services = TRUE) > 0 THEN
      DELETE FROM movers_with_location WHERE movers_with_location.piano = false;
    END IF;
    --FILTER BY MIS
    IF (SELECT COUNT(*) FROM mp_addresses WHERE role_in_plan = 'drop_off') = 0 THEN
      DELETE FROM movers_with_location WHERE movers_with_location.warehousing = false;
    END IF;
    --FILTER BY SIT
    IF (SELECT storage_move_out_date FROM mp) IS NOT NULL THEN
      DELETE FROM movers_with_location WHERE movers_with_location.storage_in_transit = false;
    END IF;
    --FILTER BY PHONE REQUEST
    IF (SELECT count(*) FROM onsite_requests WHERE move_plan_id = mp_id AND type = 'InHomeRequest') > 0 THEN
      DELETE FROM movers_with_location WHERE movers_with_location.onsites = false;
    END IF;
    --FILTER BY ONSITE REQUEST
    IF (SELECT count(*) FROM onsite_requests WHERE move_plan_id = mp_id AND type = 'PhoneRequest') > 0 THEN
      DELETE FROM movers_with_location WHERE movers_with_location.callback = false;
    END IF;
    --FILTER BY CRATING
    IF num_crating > 0 THEN
      DELETE FROM movers_with_location WHERE movers_with_location.crating = false;
    END IF;
    --FILTER BY DISSASEMBLY/ASSEMBLY
    IF (SELECT count(*) FROM mp_ii WHERE assembly_required = TRUE) > 0 THEN
      DELETE FROM movers_with_location WHERE movers_with_location.disassembly_assembly = false;
    END IF;
    --FILTER BY WALL REMOVAL
    IF (SELECT count(*) FROM mp_ii WHERE wall_removal_required = TRUE) > 0 THEN
      DELETE FROM movers_with_location WHERE movers_with_location.wall_dismounting = false;
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
        SUM(GREATEST((cicf.cubic_feet * crating_pc.cents_per_cubic_foot_of_crating / 100),(crating_pc.minimum_cents_per_item_crated / 100)))
      ELSE
        0
      END) AS crating_cost,
      crating_pc.id AS crating_pc_id
    FROM crating_item_cubic_feet AS cicf
    JOIN price_charts AS crating_pc
    ON crating_pc.id IN (SELECT crating_mwlabr.latest_pc_id FROM movers_with_location_and_balancing_rate AS crating_mwlabr)
    GROUP BY crating_pc_id);
    --CARDBOARD/PACKING/UNPACKING COST BY PRICE_CHART
    DROP TABLE IF EXISTS cb_p_up_cost_pc;
    CREATE TEMP TABLE cb_p_up_cost_pc AS (SELECT
      CASE WHEN box_delivery_dow IS NOT NULL THEN
        SUM(cents_for_cardboard/100 * quantity) +
        --FIGURE OUT REDICULOUS BOX_DELIVERY_FEE (WHAT ARE THESE PATTERNS???????)
        (CASE box_delivery_dow
          WHEN 1 THEN cb_p_up_pc.box_delivery_fee_monday
          WHEN 2 THEN cb_p_up_pc.box_delivery_fee_thursday
          WHEN 3 THEN cb_p_up_pc.box_delivery_fee_wednesday
          WHEN 4 THEN cb_p_up_pc.box_delivery_fee_thursday
          WHEN 5 THEN cb_p_up_pc.box_delivery_fee_friday
          WHEN 6 THEN cb_p_up_pc.box_delivery_fee_saturday
          WHEN 7 THEN cb_p_up_pc.box_delivery_fee_sunday
        ELSE
          0
        END)
      ELSE 0
      END AS cardboard_cost,
      CASE WHEN (SELECT follow_up_packing_service_id FROM mp) = 1
                OR (SELECT initial_packing_service_id FROM mp) = 1
                OR (SELECT follow_up_packing_service_id FROM mp) = 2
                OR (SELECT initial_packing_service_id FROM mp) = 2
                THEN SUM(cents_for_packing/100 * quantity) + cb_p_up_pc.packing_flat_fee
      ELSE 0
      END AS packing_cost,
      CASE WHEN (SELECT follow_up_packing_service_id FROM mp) = 2
                OR (SELECT initial_packing_service_id FROM mp) = 2
                THEN SUM(cents_for_unpacking/100 * quantity)
      ELSE 0
      END AS unpacking_cost,
      cb_p_up_pc.id AS cb_p_up_pc_id
    FROM mp_bi
    JOIN price_charts AS cb_p_up_pc
    ON cb_p_up_pc.id IN (SELECT cb_p_up_mwlabr.latest_pc_id FROM movers_with_location_and_balancing_rate AS cb_p_up_mwlabr)
    JOIN box_type_rates AS btr
    ON cb_p_up_pc.id = btr.price_chart_id AND btr.box_type_id = mp_bi.box_type_id
    GROUP BY cb_p_up_pc.id, cb_p_up_pc.packing_flat_fee);
    --DO ALL THE PRICING STUFF (oh boi)
    DROP TABLE IF EXISTS movers_and_pricing;
    CREATE TEMP TABLE movers_and_pricing AS (
--TOTAL
  --SUBTOTAL
    --SUM BEFORE ADJUSTMENT
      --MOVING COST ADJUSTED
        --ITEM COST
      SELECT
        ROUND(
          ((CASE WHEN mwlabr.location_type = 'local' AND (SELECT storage_move_out_date FROM mp) IS NOT NULL  THEN
            item_cubic_feet * mwlabr.local_cents_per_cubic_foot / 100 * 2
          ELSE
            item_cubic_feet * mwlabr.local_cents_per_cubic_foot / 100
          END) +
          --BOX COST
          (CASE WHEN mwlabr.location_type = 'local' AND (SELECT storage_move_out_date FROM mp) IS NOT NULL  THEN
            box_cubic_feet * mwlabr.local_cents_per_cubic_foot / 100 * 2
          ELSE
            box_cubic_feet * mwlabr.local_cents_per_cubic_foot / 100
          END) +
          --HEIGHT COST
          (total_cubic_feet * price_charts.cents_per_cubic_foot_per_flight_of_stairs * num_stairs / 100) +
          --ITEM HANDLING COST
          (COALESCE(ihc.item_handling, 0)) +
          --BOX HANDLING COST
          (COALESCE(bhc.box_handling, 0))) *
          --MULTIPLY ABOVE BY BALANCING RATE
          balancing_rate.rate,
        2) AS moving_cost_adjusted,
        --TRAVEL COST ADJUSTED
        ROUND(
          --TRUCK COST
          ((CASE WHEN mwlabr.location_type = 'local' AND (SELECT storage_move_out_date FROM mp) IS NOT NULL  THEN
            Cast(price_charts.cents_per_truck / 100 * 2 as numeric)
          ELSE
            Cast(price_charts.cents_per_truck / 100 as numeric)
          END) +
          --DISTANCE COST ADJUSTED
          (CASE WHEN travel_plan_miles.distance_minus_free < 0 THEN
            0.00
          ELSE
            travel_plan_miles.distance_minus_free * price_charts.cents_per_mile / 100
          END)
          +
          --HANDLE EXTRA LONG DISTANCE COSTS
          (CASE WHEN mwlabr.location_type = 'local' THEN
            0.00
          ELSE
            --LONG DISTANCE CUBIC FEET COST WITH PRICING TIERS
            ((total_cubic_feet * mwlabr.cents_per_cubic_foot / 100 * COALESCE(long_distance_tiers_coeffienct,1) ) + COALESCE(mwlabr.extra_fee,0))
            +
            --EXTRA DROP OFF LOCAL COST
            (CASE WHEN edo_state IS NULL THEN
              0.00
            ELSE
              (SELECT * FROM distance_in_miles(do_key,edo_key)) * price_charts.cents_per_mile / 100
            END)
          END)) *
          --MULTIPLY ABOVE BY BALANCING RATE
          balancing_rate.rate,
        2) AS travel_cost_adjusted,
      --SPECIAL HANDLING COST ADJUSTED
        --CRATING COST
        (crating_cost  +
        --CARPENTRY COST
        (CASE WHEN num_carpentry > 0 THEN
          (minimum_carpentry_cost_per_hour_in_cents / 100 * price_charts.special_handling_hours)
        ELSE
          0.00
        END
        ))* balancing_rate.rate AS special_handling_cost_adjusted,
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
          (price_charts.storage_fee / 100 * total_cubic_feet)
        ELSE
          0.00
        END),2) AS storage_cost,
      --PACKING COST ADJUSTED
        ROUND(((cb_p_up_cost_pc.packing_cost + cb_p_up_cost_pc.unpacking_cost) * balancing_rate.rate),2) AS packing_cost_adjusted,
      --CARDBOARD COST ADJUSTED
        ROUND(((cb_p_up_cost_pc.cardboard_cost) * balancing_rate.rate),2) AS cardboard_cost_adjusted,
      --SURCHARGE CUBIC FEET COST ADJUSTED
      --COI CHARGES COST ADJUSTED
      --SIZE SURCHARGE COST ADJUSTED
    --ADMIN ADJUSTMENT BEFORE DISCOUNT
  --ALL DISCOUNTS
    --MOVER SPECIAL DISCOUNT
    --FACEBOOK DISCOUNT
    --TWITTER DISCOUNT
    --COUPON DISCOUNT
  --ADMIN ADJUSTMENT AFTER DISCOUNT
        mwlabr.*
      FROM movers_with_location_and_balancing_rate AS mwlabr
      JOIN price_charts
        ON mwlabr.latest_pc_id = price_charts.id
      JOIN travel_plan_miles
        ON mwlabr.latest_pc_id = travel_plan_miles.latest_pc_id
      JOIN crating_cost_pc
        ON crating_cost_pc.crating_pc_id = mwlabr.latest_pc_id
      JOIN cb_p_up_cost_pc
        ON cb_p_up_cost_pc.cb_p_up_pc_id = mwlabr.latest_pc_id
      JOIN (SELECT
        (1 + (
          (CASE WHEN mov_time = 'am' THEN
            mwlabr_br.balancing_rate_primary
            ELSE
            coalesce(mwlabr_br.balancing_rate_secondary, mwlabr_br.balancing_rate_primary)
            END
          ) / 100)) AS rate, mwlabr_br.latest_pc_id FROM movers_with_location_and_balancing_rate AS mwlabr_br) AS balancing_rate
      ON balancing_rate.latest_pc_id = mwlabr.latest_pc_id
        LEFT JOIN (
          SELECT ihc.price_chart_id AS ihc_pc, sum(cost) AS item_handling
          FROM mp_ii
          JOIN item_handling_charges as ihc
          ON mp_ii.inventory_item_id = ihc.item_id
          AND ihc.item_type = 'InventoryItem'
          AND ihc.price_chart_id IN (SELECT DISTINCT items_mwlabr.price_chart_id FROM movers_with_location_and_balancing_rate AS items_mwlabr)
          GROUP BY ihc_pc) AS ihc
        ON ihc_pc = mwlabr.latest_pc_id
      LEFT JOIN (
          SELECT bhc.price_chart_id AS bhc_pc, sum(cost * quantity) AS box_handling
          FROM mp_bi
          JOIN item_handling_charges as bhc
          ON mp_bi.box_type_id = bhc.item_id
          AND bhc.item_type = 'BoxType'
          AND bhc.price_chart_id IN (SELECT DISTINCT boxes_mwlabr.price_chart_id FROM movers_with_location_and_balancing_rate AS boxes_mwlabr)
          GROUP BY bhc_pc) AS bhc
        ON bhc_pc = mwlabr.latest_pc_id
      LEFT JOIN (
          SELECT (
             SUM(rate_part) + 100)/100 AS long_distance_tiers_coeffienct,
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
        ON long_distance_price_chart_id = mwlabr.latest_pc_id);




--             cents_per_mile,
--             cents_per_cubic_foot_of_crating, minimum_cents_per_item_crated,
--             cents_per_truck, free_miles, cents_per_cubic_foot_per_flight_of_stairs,
--             zip, minimum_carpentry_cost_per_hour_in_cents, special_handling_hours,
--             minimum_local_cubic_feet, valuation_minimum_value,
--             default_insurance_valuation_per_pound, latitude, longitutde,
--             coi_charge_cents, extra_stop_value, storage_fee, range, drop_off_mileage,
--             minimum_job_distance, box_delivery_range, minimum_long_distance_cubic_feet,
--             storage_unloading_in_cents, storage_loading_in_cents, storage_padding_in_cents,
--             apply_cubic_feet_tier_to_local_moves, apply_distance_tier_to_local_moves,
--             self_storage_enabled, self_storage_months_deposit, self_storage_free_months,
--             long_carry_free_feet, interstate_toll, box_delivery_fee_sunday,
--             box_delivery_fee_monday, box_delivery_fee_tuesday, box_delivery_fee_wednesday,
--             box_delivery_fee_thrusday, box_delivery_fee_friday, box_delivery_fee_saturday,
--             long_carry_fee, max_cubic_feet, packing_flat_fee

    RETURN QUERY SELECT * FROM movers_and_pricing ORDER BY (movers_and_pricing.moving_cost_adjusted +
                                                            movers_and_pricing.travel_cost_adjusted +
                                                            movers_and_pricing.special_handling_cost_adjusted) ASC ;
    END; $$
  LANGUAGE plpgsql;
