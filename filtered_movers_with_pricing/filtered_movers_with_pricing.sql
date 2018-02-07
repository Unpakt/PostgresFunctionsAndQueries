    SELECT * FROM filtered_movers_with_pricing('c3c7ff12-0b81-11e8-e3bd-19183eca35e3');
    DROP FUNCTION IF EXISTS filtered_movers_with_pricing(move_plan_param VARCHAR);
    CREATE FUNCTION filtered_movers_with_pricing(move_plan_param VARCHAR)
    RETURNS TABLE(
              mover_name varchar, mover_id integer,
              pick_up_mileage numeric, drop_off_mileage numeric,
              extra_stop_enabled boolean,
              packing boolean,unpacking boolean, box_delivery boolean,
              piano boolean, storage boolean, onsites boolean,callback boolean,
              crating boolean,disassembly_assembly boolean,
              wall_dismounting boolean, box_delivery_range numeric,
              cents_per_cubic_foot numeric, pu_lat numeric,
              pu_long numeric, latest_pc_id integer,
               mover_earth earth, mover_location_id integer,
              price_chart_id integer, local_cents_per_cubic_foot numeric,
              location_type varchar, maximum_delivery_days integer,
              minimum_delivery_days integer , dedicated boolean,
              extra_fee numeric, range numeric, partially_active boolean,
              location_latitude DOUBLE PRECISION, location_longitude DOUBLE PRECISION, distance_in_miles DOUBLE PRECISION,
              balancing_rate_primary NUMERIC, balancing_rate_secondary NUMERIC, net_am BIGINT, net_pm BIGINT) AS $$
    DECLARE mov_date date;DECLARE mov_time varchar;DECLARE num_stairs integer;
    DECLARE mp_cubic_feet numeric;
    DECLARE mp_id integer;
    DECLARE pu_state varchar;DECLARE epu_state varchar;DECLARE do_state varchar;DECLARE edo_state varchar;
    DECLARE pu_earth earth;DECLARE epu_earth earth;DECLARE do_earth earth;DECLARE edo_earth earth;
      BEGIN
        --SET GENERAL VARIABLES
        mp_id := (SELECT uuidable_id FROM uuids WHERE uuids.uuid = $1 AND uuidable_type = 'MovePlan');
        DROP TABLE IF EXISTS mp;
        CREATE TEMP TABLE mp AS (SELECT * FROM move_plans WHERE move_plans.id = mp_id);
        mov_date := (SELECT move_date FROM mp);
        mov_time :=(SELECT CASE WHEN mp.move_time LIKE '%PM%' THEN 'pm' ELSE 'am' END FROM mp );
        num_stairs := (
          SELECT sum(flights_of_stairs) FROM (
            SELECT
              heights.flights_of_stairs
            FROM addresses
            JOIN heights
              ON addresses.height_id = heights.id
              AND addresses.move_plan_id = mp_id
              AND addresses.role_in_plan IN ('drop_off', 'pick_up') ) as stairs);
        --FILTER BY "MOVER IS LIVE"
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
        --GRAB MOVE PLAN INVENTORY ITEMS
        DROP TABLE IF EXISTS mp_ii;
        CREATE TEMP TABLE mp_ii AS SELECT move_plan_inventory_items.id as mpii_id,
                                   move_plan_id, inventory_item_id, item_group_id,
                                   assembly_required, wall_removal_required, crating_required, is_user_selected, requires_piano_services,
                                  inventory_items.name as item_name, icon_css_class, cubic_feet, is_user_generated, description
                                   FROM move_plan_inventory_items
                                   JOIN inventory_items
                                     ON move_plan_inventory_items.inventory_item_id = inventory_items.id
                                     AND move_plan_id = mp_id;
        mp_cubic_feet := (SELECT SUM(cubic_feet) FROM mp_ii);
        --SET ADDRESS VARIABLES FOR FUTURE USE
        DROP TABLE IF EXISTS mp_addresses;
        CREATE TEMP TABLE mp_addresses AS SELECT * FROM addresses WHERE move_plan_id = mp_id;
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
        --FILTER BY HAUL TYPE AND DISTANCE
        DROP TABLE IF EXISTS movers_by_haul;
        CREATE TEMP TABLE movers_by_haul (mover_name varchar, mover_id integer, pick_up_mileage numeric, drop_off_mileage numeric,
        extra_stop_enabled boolean, packing boolean, unpacking boolean, box_delivery boolean, piano boolean, storage boolean,
        onsites boolean, callback boolean, crating boolean, disassembly_assembly boolean, wall_dismounting boolean,
        box_delivery_range numeric, local_cents_per_cubic_foot numeric, pu_lat numeric, pu_long numeric, latest_pc_id integer, mover_earth earth);
        IF (SELECT count(distinct(state)) FROM mp_addresses) > 1 THEN
            INSERT INTO movers_by_haul SELECT
              potential_movers.mover_name, potential_movers.id AS mover_id,
              price_charts.range as pick_up_mileage, price_charts.drop_off_mileage,
              price_charts.extra_stop_enabled,
              additional_services.packing,additional_services.unpacking, additional_services.box_delivery,
              additional_services.piano, additional_services.storage, additional_services.onsites,additional_services.callback,
              additional_services.crating,additional_services.disassembly_assembly,
              additional_services.wall_dismounting, price_charts.box_delivery_range,
              price_charts.cents_per_cubic_foot as local_cents_per_cubic_foot,
              price_charts.latitude as pu_lat,  price_charts.longitude as pu_long, potential_movers.latest_pc_id,
              (SELECT * FROM ll_to_earth(price_charts.latitude, price_charts.longitude)) AS mover_earth
            FROM potential_movers
              JOIN price_charts
                ON price_charts.id = potential_movers.latest_pc_id
                AND price_charts.us_dot IS NOT NULL
                AND price_charts.us_dot <> ''
                AND price_charts.usa_interstate_moves = 't'
                AND (price_charts.range * 1609.34) >= (SELECT * FROM earth_distance(
                    ll_to_earth(price_charts.latitude, price_charts.longitude),
                    pu_earth))
                AND (price_charts.minimum_job_distance * 1609.34) <= (SELECT * FROM earth_distance(
                    do_earth,pu_earth))
                AND (price_charts.max_cubic_feet IS NULL OR mp_cubic_feet <= price_charts.max_cubic_feet)
              JOIN additional_services
                ON additional_services.price_chart_id = price_charts.id;
        ELSE
            INSERT INTO movers_by_haul SELECT
              potential_movers.mover_name, potential_movers.id AS mover_id,
              price_charts.range as pick_up_mileage, price_charts.drop_off_mileage,
              price_charts.extra_stop_enabled,
              additional_services.packing,additional_services.unpacking, additional_services.box_delivery,
              additional_services.piano, additional_services.storage, additional_services.onsites,additional_services.callback,
              additional_services.crating,additional_services.disassembly_assembly,
              additional_services.wall_dismounting, price_charts.box_delivery_range,
              price_charts.cents_per_cubic_foot as local_cents_per_cubic_foot,
              price_charts.latitude as pu_lat,  price_charts.longitude as pu_long, potential_movers.latest_pc_id,
              (SELECT * FROM ll_to_earth(price_charts.latitude, price_charts.longitude)) AS mover_earth
            FROM potential_movers
              JOIN price_charts
                ON price_charts.id = potential_movers.latest_pc_id
                AND price_charts.local_moves = true
                AND (price_charts.state_authority_1_state = pu_state OR
                     price_charts.state_authority_2_state = pu_state OR
                     price_charts.state_authority_3_state = pu_state OR
                     price_charts.state_authority_4_state = pu_state )
                AND (price_charts.range * 1609.34) >= (SELECT * FROM earth_distance(
                    ll_to_earth(price_charts.latitude, price_charts.longitude),
                    pu_earth))
                AND (price_charts.minimum_job_distance * 1609.34) <= (SELECT * FROM earth_distance(
                    do_earth,pu_earth))
                AND (price_charts.max_cubic_feet IS NULL OR mp_cubic_feet <= price_charts.max_cubic_feet)
              JOIN additional_services
                ON additional_services.price_chart_id = price_charts.id;
        END IF;
        --FILTER BY EXTRA PICK UP
        IF (SELECT extra_pick_up_enabled FROM mp) = true THEN
          DELETE FROM movers_by_haul WHERE (SELECT * FROM earth_distance(epu_earth,mover_earth)) > pick_up_mileage * 1609.34;
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
            earth_distance(ll_to_earth(movers_by_haul.pu_lat,movers_by_haul.pu_long),do_earth)/1609.34 AS distance_in_miles
          FROM movers_by_haul
          WHERE (SELECT * FROM earth_distance(ll_to_earth(movers_by_haul.pu_lat, movers_by_haul.pu_long), do_earth)) <= movers_by_haul.drop_off_mileage * 1609.34);
        --FIND ALL LONG DISTANCE MOVER LOCATIONS
        DROP TABLE IF EXISTS mover_state_locations;
        CREATE TEMP TABLE mover_state_locations AS (
          SELECT mover_locations.id as mover_location_id,
            mover_locations.price_chart_id, mover_locations.cents_per_cubic_foot,
            mover_locations.location_type, mover_locations.maximum_delivery_days,
            mover_locations.minimum_delivery_days, mover_locations.dedicated,
            mover_locations.extra_fee, mover_locations.range, mover_locations.partially_active,
            mover_locations.latitude as location_latitude, mover_locations.longitude as location_longitude,
            CAST(NULL AS NUMERIC) as distance_in_miles
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
        DROP TABLE IF EXISTS all_mover_locations;
        CREATE TEMP TABLE all_mover_locations AS (
          SELECT * FROM mover_local_locations
          UNION ALL SELECT * FROM mover_full_state_locations
          UNION ALL SELECT * FROM mover_city_locations);
        DROP TABLE IF EXISTS movers_with_location;
        CREATE TEMP TABLE movers_with_location AS
          SELECT * FROM movers_by_haul  JOIN all_mover_locations on all_mover_locations.price_chart_id =  movers_by_haul.latest_pc_id;
        --FILTER BY EXTRA DROP OFF
        IF (SELECT extra_drop_off_enabled FROM mp) = true THEN
          DELETE FROM movers_with_location WHERE extra_stop_enabled = false;
          DELETE FROM movers_with_location WHERE location_type = 'local' AND earth_distance(mover_earth,edo_earth)/1609.34 > drop_off_mileage;
          DELETE FROM movers_with_location WHERE location_type = 'state' AND earth_distance(do_earth,edo_earth)/1609.34 > MAX(50,drop_off_mileage);
          DELETE FROM movers_with_location WHERE location_type = 'city'  AND earth_distance(ll_to_earth(location_latitude,location_longitude),edo_earth)/1609.34 > range;
        END IF;
        --FILTER BY MINIMUM DISTANCE
        --FILTERING BY ADDITIONAL SERVICES
        IF (SELECT follow_up_packing_service_id FROM mp) = 1 OR (SELECT initial_packing_service_id FROM mp) = 1 THEN
          DELETE FROM movers_with_location WHERE packing = false;
        END IF;
        IF (SELECT follow_up_packing_service_id FROM mp) = 2 OR (SELECT initial_packing_service_id FROM mp) = 2 THEN
          DELETE FROM movers_with_location WHERE packing = false;
          DELETE FROM movers_with_location WHERE unpacking = false;
        END IF;
        IF (SELECT box_delivery_date FROM mp) IS NOT NULL THEN
          DELETE FROM movers_with_location WHERE box_delivery = false;
          DELETE FROM movers_with_location WHERE earth_distance(pu_earth,mover_earth) > box_delivery_range;
        END IF;
        IF (SELECT COUNT(*) FROM mp_ii WHERE requires_piano_services = TRUE) > 0 THEN
          DELETE FROM movers_with_location WHERE piano = false;
        END IF;
        IF  (SELECT COUNT(*) FROM mp_addresses WHERE role_in_plan = 'drop_off') = 0
            OR (SELECT storage_move_out_date FROM mp) IS NOT NULL THEN
          DELETE FROM movers_with_location where storage = false;
        END IF;
        IF (SELECT count(*) FROM onsite_requests WHERE move_plan_id = mp_id AND type = 'InHomeRequest') > 0 THEN
          DELETE FROM movers_with_location WHERE onsites = false;
        END IF;
        IF (SELECT count(*) FROM onsite_requests WHERE move_plan_id = mp_id AND type = 'PhoneRequest') > 0 THEN
          DELETE FROM movers_with_location WHERE callback = false;
        END IF;
        IF (SELECT count(*) FROM mp_ii where crating_required = TRUE) > 0 THEN
          DELETE FROM movers_with_location WHERE crating = false;
        END IF;
        IF (SELECT count(*) FROM mp_ii WHERE assembly_required = TRUE) > 0 THEN
          DELETE FROM movers_with_location WHERE disassembly_assembly = false;
        END IF;
        IF (SELECT count(*) FROM mp_ii WHERE wall_removal_required = TRUE) > 0 THEN
          DELETE FROM movers_with_location WHERE wall_dismounting = false;
        END IF;
        CREATE TEMP TABLE movers_with_location_and_balancing_rate AS (
                 SELECT * FROM (
                  SELECT
                  mwl.*,
                  COALESCE(adj.balancing_rate_primary, rul.balancing_rate_primary) AS balancing_rate_primary,
                  COALESCE(adj.balancing_rate_secondary, rul.balancing_rate_secondary) AS balancing_rate_secondary,
                  CASE WHEN COALESCE(adj.capacity_secondary, rul.capacity_secondary) IS NULL
                    THEN COALESCE(adj.capacity_primary, rul.capacity_primary) - COALESCE(am, 0) - COALESCE(pm, 0)
                  ELSE COALESCE(adj.capacity_secondary, rul.capacity_secondary) - COALESCE(pm, 0)
                    END as net_pm,
                  CASE WHEN COALESCE(adj.capacity_secondary, rul.capacity_secondary) IS NULL
                    THEN COALESCE(adj.capacity_primary, rul.capacity_primary) - COALESCE(am, 0) - COALESCE(pm, 0)
                  ELSE COALESCE(adj.capacity_primary, rul.capacity_primary) - COALESCE(am, 0)
                    END as net_am
                FROM movers_with_location as mwl
                LEFT JOIN(
                   SELECT *
                   FROM PUBLIC.daily_adjustments AS day_adj
                   JOIN PUBLIC.daily_adjustment_data AS adj_data
                     ON day_adj.daily_adjustment_datum_id = adj_data.id) AS adj
                 ON mov_date  = day
                  AND mwl.price_chart_id = adj.price_chart_id
                LEFT JOIN(
                   SELECT *
                   FROM PUBLIC.daily_adjustment_rules AS rul_adj
                   JOIN PUBLIC.daily_adjustment_data AS adj_data
                     ON rul_adj.daily_adjustment_datum_id = adj_data.id) AS rul
                ON weekday = EXTRACT(isodow FROM '12/15/17' :: DATE) - 1
                  AND mwl.price_chart_id = rul.price_chart_id
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
              END);





--             latitude, longitude, cents_per_cubic_foot, cents_per_mile,
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

--           CREATE TEMP TABLE daily AS (
--             SELECT balancing_rate_primary, balancing_rate_secondary FROM  daily_adjustment_data
--             JOIN daily_adjustments
--                 ON daily_adjustments.daily_adjustment_datum_id = daily_adjustment_data.id
--                 AND daily_adjustments.day = mov_date
--                 AND price_chart_id = pc_id);
--           CREATE TEMP TABLE weekly AS (
--             SELECT balancing_rate_primary, balancing_rate_secondary FROM  daily_adjustment_data
--             JOIN daily_adjustment_rules
--                 ON daily_adjustment_rules.daily_adjustment_datum_id = daily_adjustment_data.id
--                    AND daily_adjustment_rules.weekday =  EXTRACT(isodow FROM mov_date :: DATE) - 1
--                    AND price_chart_id = pc_id);
--           has_daily := (SELECT CASE WHEN count(*) > 0 THEN true ELSE false END FROM daily);
--           bal_rate := (
--             CASE has_daily
--             WHEN true THEN
--                 CASE WHEN mov_time = 'am' THEN
--                   (SELECT balancing_rate_primary FROM daily)
--                 ELSE
--                   (SELECT COALESCE(balancing_rate_secondary, balancing_rate_primary) FROM daily)
--                 END
--             ELSE
--                 CASE WHEN mov_time = 'am' THEN
--                   (SELECT balancing_rate_primary FROM weekly)
--                 ELSE
--                   (SELECT COALESCE(balancing_rate_secondary, balancing_rate_primary) FROM weekly)
--                 END
--             END
--           );

        RETURN QUERY SELECT * FROM movers_with_location_and_balancing_rate;
        END; $$
      LANGUAGE plpgsql;

