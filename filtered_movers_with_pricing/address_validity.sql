
SELECT * FROM validate_addresses(3423432,3423423,43242,23442,1);

DROP FUNCTION IF EXISTS validate_addresses(integer,integer,integer,integer,integer);
CREATE FUNCTION validate_addresses(pu_geo integer, do_geo integer, pc_id integer, epu_geo integer default NULL, edo_geo integer default NULL)
RETURNS TABLE(errors VARCHAR) AS $$

--DEFINE VARIABLES: PICKUP(pu_), EXTRA PICK UP(epu_), DROP OFF(do_), EXTRA DROP OFF(edo_)
DECLARE pu_state varchar; DECLARE pu_earth earth;
DECLARE epu_state varchar;DECLARE epu_earth earth;
DECLARE do_state varchar; DECLARE do_earth earth;
DECLARE edo_state varchar;DECLARE edo_earth earth;
DECLARE intrastate boolean;

DECLARE pc_earth varchar;DECLARE pc_state varchar;
DECLARE pc_us_dot boolean;DECLARE pc_usa_interstate_moves boolean;
DECLARE pc_range numeric;DECLARE pc_lat numeric; DECLARE pc_long numeric;
DECLARE pc_state_authority_1_state varchar;
DECLARE pc_state_authority_2_state varchar;
DECLARE pc_state_authority_3_state varchar;
DECLARE pc_state_authority_4_state varchar;
DECLARE pc_minimum_job_distance numeric;
DECLARE pc_extra_stop_enabled BOOLEAN;
DECLARE pc_drop_off_mileage BOOLEAN;

DECLARE
  BEGIN
    --SET VARIABLES
    pu_state  := (SELECT state FROM geocodes WHERE id = pu_geo);
    do_state  := (SELECT state FROM geocodes WHERE id = do_geo);
    epu_state := (SELECT state FROM geocodes WHERE id = epu_geo);
    edo_state := (SELECT state FROM geocodes WHERE id = edo_geo);
    pc_state  := (SELECT state FROM zip_codes WHERE zip = (SELECT zip FROM price_charts WHERE id = pc_id));
    pu_earth  := (SELECT * FROM ll_to_earth(
        (SELECT latitude FROM geocodes WHERE id = pu_geo),
        (SELECT longitude FROM geocodes WHERE id = pu_geo)));
    epu_earth := (SELECT * FROM ll_to_earth(
        (SELECT latitude FROM geocodes WHERE id = do_geo),
        (SELECT longitude FROM geocodes WHERE id = do_geo)));
    do_earth  := (SELECT * FROM ll_to_earth(
        (SELECT latitude FROM geocodes WHERE id = epu_geo),
        (SELECT longitude FROM geocodes WHERE id = epu_geo)));
    edo_earth := (SELECT * FROM ll_to_earth(
        (SELECT latitude FROM geocodes WHERE id = edo_geo),
        (SELECT longitude FROM geocodes WHERE id = edo_geo)));
    pc_earth  := (SELECT * FROM ll_to_earth(
        (SELECT latitude FROM price_charts WHERE id = pc_id),
        (SELECT longitude FROM price_charts WHERE id = pc_id)));
    intrastate := (SELECT
      (pu_state = COALESCE(do_state,pc_state)) AND
      CASE WHEN epu_geo IS NOT NULL THEN (pu_state = epu_state) ELSE TRUE END AND
      CASE WHEN edo_geo IS NOT NULL THEN (pu_state = edo_state) ELSE TRUE END
    );
    pc_us_dot := (SELECT us_dot <> '' OR us_dot IS NOT NULL FROM price_charts WHERE id = pc_id);
    pc_usa_interstate_moves := (SELECT usa_interstate_moves = 't' FROM price_charts WHERE id = pc_id);
    pc_range := (SELECT range * 1609.34 FROM price_charts WHERE id = pc_id);
    pc_lat := (SELECT latitude FROM price_charts WHERE id = pc_id);
    pc_long := (SELECT longitude FROM price_charts WHERE id = pc_id);
    pc_state_authority_1_state := (SELECT state_authority_1_state FROM price_charts WHERE id = pc_id);
    pc_state_authority_2_state := (SELECT state_authority_2_state FROM price_charts WHERE id = pc_id);
    pc_state_authority_3_state := (SELECT state_authority_3_state FROM price_charts WHERE id = pc_id);
    pc_state_authority_4_state := (SELECT state_authority_4_state FROM price_charts WHERE id = pc_id);
    pc_minimum_job_distance := (SELECT minimum_job_distance * 1609.34 FROM price_charts WHERE id = pc_id);
    pc_extra_stop_enabled := (SELECT extra_stop_enabled FROM price_charts WHERE id = pc_id);
		pc_drop_off_mileage := (SELECT drop_off_mileage * 1609.34 FROM price_charts WHERE id = pc_id);

		DROP TABLE IF EXISTS address_errors;
		CREATE TEMP TABLE address_errors (errors varchar);


		IF  pc_range >= (SELECT * FROM earth_distance(pc_earth, pu_earth)) THEN
			INSERT INTO address_errors VALUES ('Pick up location is out of range');
		END IF;

    IF intrastate = false AND ( pc_us_dot = false OR pc_usa_interstate_moves = false) THEN
      INSERT INTO address_errors VALUES ('This mover does not support interstate moves');
    END IF;

    IF intrastate = true AND pc_state_authority_2_state <> pu_state AND pc_state_authority_2_state <> pu_state AND pc_state_authority_3_state <> pu_state AND pc_state_authority_4_state <> pu_state THEN
      INSERT INTO address_errors VALUES ('This mover does not support this state');
		END IF;

		IF  pu_state in ('IL', 'PA') AND pc_minimum_job_distance >= (SELECT earth_distance(COALESCE(do_earth, pc_earth),pu_earth)) THEN
			INSERT INTO address_errors VALUES ('This mover does not support this pick up location');
    END IF;

    IF epu_geo IS NOT NULL AND ((SELECT FROM earth_distance(epu_earth, pc_earth)) > pc_range OR pc_extra_stop_enabled) THEN
			INSERT INTO address_errors VALUES ('This mover does not support this extra pick up location');
    END IF;

    IF do_geo IS NOT NULL AND (SELECT FROM earth_distance(pc_earth,do_earth)) >= pc_drop_off_mileage THEN
			SELECT count(*) FROM mover_locations WHERE state_code in (do_state,edo_state) AND active = true AND location_type = 'state' and price_chart_id = pc_id)
		END IF;

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

RETURN QUERY SELECT 'error';
END; $$
LANGUAGE plpgsql;