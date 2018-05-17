
SELECT * FROM validate_addresses(20644,20644,14457);
DROP FUNCTION IF EXISTS validate_addresses(integer,integer,integer,integer,integer);
CREATE FUNCTION validate_addresses(pu_geo integer, do_geo integer, pc_id integer, epu_geo integer default NULL, edo_geo integer default NULL)
RETURNS TABLE(errors VARCHAR) AS $$

--DEFINE VARIABLES: PICKUP(pu_), EXTRA PICK UP(epu_), DROP OFF(do_), EXTRA DROP OFF(edo_)
DECLARE pu_state varchar; DECLARE pu_earth earth;
DECLARE epu_state varchar;DECLARE epu_earth earth;
DECLARE do_state varchar; DECLARE do_earth earth;
DECLARE edo_state varchar;DECLARE edo_earth earth;
DECLARE intrastate boolean;

DECLARE pc_earth earth;DECLARE pc_state varchar;
DECLARE pc_us_dot boolean;DECLARE pc_usa_interstate_moves boolean;
DECLARE pc_range numeric;DECLARE pc_lat numeric; DECLARE pc_long numeric;
DECLARE pc_state_authority_1_state varchar;
DECLARE pc_state_authority_2_state varchar;
DECLARE pc_state_authority_3_state varchar;
DECLARE pc_state_authority_4_state varchar;
DECLARE pc_minimum_job_distance numeric;
DECLARE pc_extra_stop_enabled BOOLEAN;
DECLARE pc_drop_off_mileage NUMERIC;
DECLARE pc_local_moves BOOLEAN;

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
    do_earth  := (SELECT * FROM ll_to_earth(
        (SELECT latitude FROM geocodes WHERE id = do_geo),
        (SELECT longitude FROM geocodes WHERE id = do_geo)));
    epu_earth := (SELECT * FROM ll_to_earth(
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
		pc_local_moves := (SELECT local_moves FROM price_charts WHERE id = pc_id);

		DROP TABLE IF EXISTS address_errors;
		CREATE TEMP TABLE address_errors (errors varchar);


		IF  pc_range < (SELECT * FROM earth_distance(pc_earth, pu_earth)) THEN
			INSERT INTO address_errors VALUES ('Pick up location is out of range');
		END IF;

    IF intrastate = false AND ( pc_us_dot = false OR pc_usa_interstate_moves = false) THEN
      INSERT INTO address_errors VALUES ('This mover does not support interstate moves');
    END IF;

    IF intrastate = true AND pc_state_authority_1_state <> pu_state AND pc_state_authority_2_state <> pu_state AND pc_state_authority_3_state <> pu_state AND pc_state_authority_4_state <> pu_state THEN
      INSERT INTO address_errors VALUES ('This mover does not support this state: ' || pu_state);
		END IF;

		IF pu_state in ('IL', 'PA') AND pc_minimum_job_distance >= (SELECT earth_distance(COALESCE(do_earth, pc_earth),pu_earth)) THEN
			INSERT INTO address_errors VALUES ('This mover does not support this pick up location');
    END IF;

    IF epu_geo IS NOT NULL AND ((SELECT earth_distance(epu_earth, pc_earth)) > pc_range OR pc_extra_stop_enabled) THEN
			INSERT INTO address_errors VALUES ('This mover does not support this extra pick up location');
    END IF;

		IF do_geo IS NULL and pc_local_moves = FALSE THEN
			INSERT INTO address_errors VALUES ('This mover does not support storage');
		END IF;
		IF do_geo IS NOT NULL AND (SELECT earth_distance(pc_earth,do_earth)) > pc_drop_off_mileage THEN
	      --FIND ALL LONG DISTANCE MOVER LOCATIONS
		    DROP TABLE IF EXISTS pc_state_locations;
		    CREATE TEMP TABLE pc_state_locations AS (
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
		            AND mover_locations.price_chart_id = pc_id);

		    --FIND ALL FULL COVERAGE LOCATIONS
		    DROP TABLE IF EXISTS pc_full_state_locations;
		    CREATE TEMP TABLE pc_full_state_locations AS (
		      SELECT pc_state_locations.mover_location_id,
		        pc_state_locations.price_chart_id, pc_state_locations.cents_per_cubic_foot,
		        pc_state_locations.location_type, pc_state_locations.maximum_delivery_days,
		        pc_state_locations.minimum_delivery_days, pc_state_locations.dedicated,
		        pc_state_locations.extra_fee, pc_state_locations.range, pc_state_locations.partially_active,
		        CAST(NULL AS NUMERIC) as location_latitude, CAST(NULL AS NUMERIC) AS location_longitude,
		        CAST(NULL AS NUMERIC) AS distance_in_miles
		      FROM pc_state_locations
		      WHERE pc_state_locations.partially_active = false);

		    --FIND ALL PARTIAL COVERAGE LOCATIONS SELECTING THE LOCATION THAT IS CLOSEST AND ALSO WITHIN RANGE
		    DROP TABLE IF EXISTS pc_city_locations;
		    CREATE TEMP TABLE pc_city_locations AS (
		      SELECT closest_locations.id as mover_location_id,
		        closest_locations.price_chart_id, closest_locations.cents_per_cubic_foot,
		        closest_locations.location_type, closest_locations.maximum_delivery_days,
		        closest_locations.minimum_delivery_days, closest_locations.dedicated,
		        closest_locations.extra_fee, closest_locations.range, closest_locations.partially_active,
		        closest_locations.latitude as location_latitude, closest_locations.longitude AS location_longitude,
		        closest_locations.distance_in_miles
		      FROM (
		      SELECT * FROM (SELECT *, rank()
		        OVER (PARTITION BY all_valid_locations.price_chart_id ORDER BY earth_distance(ll_to_earth(latitude,longitude),do_earth) ASC, all_valid_locations.cents_per_cubic_foot DESC) as rank
		       FROM
		      (SELECT mover_locations.*, earth_distance(ll_to_earth(latitude,longitude),do_earth)/1609.34 AS distance_in_miles
		          FROM public.mover_locations
		            WHERE state_code IN (do_state, edo_state)
		            AND mover_locations.active = TRUE
		            AND mover_locations.location_type='city'
		            AND mover_locations.price_chart_id IN (SELECT DISTINCT pc_state_locations.price_chart_id FROM pc_state_locations WHERE pc_state_locations.partially_active = true)
		            AND mover_locations.price_chart_id NOT IN (SELECT DISTINCT pc_full_state_locations.price_chart_id FROM pc_full_state_locations)) AS all_valid_locations
		      WHERE all_valid_locations.distance_in_miles <= all_valid_locations.range) AS ranked_locations WHERE rank = 1) AS closest_locations);

		    --UNION LOCAL,STATE,CITY LOCATIONS
		    DROP TABLE IF EXISTS all_pc_locations;
		    CREATE TEMP TABLE all_pc_locations AS (
		      SELECT * FROM pc_full_state_locations
		      UNION ALL SELECT * FROM pc_city_locations);
		    IF (SELECT count(*) FROM all_pc_locations) = 0 THEN
		     INSERT INTO address_errors VALUES ('This mover does not support this drop off location');
		    ELSIF edo_geo IS NOT NULL THEN
		      DELETE FROM all_pc_locations WHERE location_type = 'state' AND earth_distance(do_earth,edo_earth)/1609.34 > GREATEST(50.0,pc_drop_off_mileage);
		      DELETE FROM all_pc_locations WHERE location_type = 'city'  AND earth_distance(ll_to_earth(location_latitude,location_longitude),edo_earth)/1609.34 > range;
		       IF (SELECT count(*) FROM all_pc_locations) = 0 THEN
		        INSERT INTO address_errors VALUES ('This mover does not support this extra drop off location');
		       END IF;
	      END IF;
			END IF;

--     --FILTER BY EXTRA DROP OFF
--     IF (SELECT mp.extra_drop_off_enabled FROM mp) = true THEN
--       DELETE FROM movers_with_location WHERE movers_with_location.extra_stop_enabled = false;
--       DELETE FROM movers_with_location WHERE movers_with_location.location_type = 'local' AND earth_distance(movers_with_location.mover_earth,edo_earth)/1609.34 > movers_with_location.drop_off_mileage;
--       DELETE FROM movers_with_location WHERE movers_with_location.location_type = 'state' AND earth_distance(do_earth,edo_earth)/1609.34 > GREATEST(50.0,movers_with_location.drop_off_mileage);
--       DELETE FROM movers_with_location WHERE movers_with_location.location_type = 'city'  AND earth_distance(ll_to_earth(movers_with_location.location_latitude,movers_with_location.location_longitude),edo_earth)/1609.34 > movers_with_location.range;
--     END IF;
--
--         --RAISE NO MOVER FOUND ERROR
--         IF (SELECT COUNT(*) FROM movers_with_location) = 0 THEN
--           RAISE EXCEPTION 'No movers can support this extra drop off location';
--         END IF;
--
--     --FILTER BY EXTRA PICK UP
--     IF (SELECT mp.extra_pick_up_enabled FROM mp) = true THEN
--       DELETE FROM movers_with_location WHERE movers_with_location.extra_stop_enabled = false;
--     END IF;
--
--         --RAISE NO MOVER FOUND ERROR
--         IF (SELECT COUNT(*) FROM movers_with_location) = 0 THEN
--           RAISE EXCEPTION 'No movers can support this extra pick up location';
--         END IF;

RETURN QUERY SELECT * FROM address_errors;
END; $$
LANGUAGE plpgsql;