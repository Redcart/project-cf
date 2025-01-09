WITH 

list_stations AS (

  SELECT DISTINCT
    station_id
  FROM `inspired-victor-442419-j3.publibike.stations`

),

list_timestamp AS (

  SELECT
    ingestion_time
  FROM 
    UNNEST(
      GENERATE_TIMESTAMP_ARRAY(
        TIMESTAMP_SUB(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), HOUR), INTERVAL 55 MINUTE), 
        TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), HOUR),
      INTERVAL 5 MINUTE)
    ) AS ingestion_time

),

list_stations_timestamp AS (

  SELECT
    list_stations.station_id,
    list_timestamp.ingestion_time
  FROM list_stations
  CROSS JOIN list_timestamp

),

available_mechanic_bikes AS (

  SELECT 
    station_id,
    ingestion_time,
    vehicle_type_id,
    COUNT(*) AS nb_bikes_available_mechanic
  FROM `inspired-victor-442419-j3.publibike.capacity`
  WHERE vehicle_type_id = "1"
  GROUP BY station_id, vehicle_type_id, ingestion_time
  
),

available_electric_bikes AS (

  SELECT 
    station_id,
    ingestion_time,
    vehicle_type_id,
    COUNT(*) AS nb_bikes_available_electric
  FROM `inspired-victor-442419-j3.publibike.capacity`
  WHERE vehicle_type_id = "2"
  GROUP BY station_id, vehicle_type_id, ingestion_time
  
),

stations_with_nb_available_bikes AS (

  SELECT 
    list_stations_timestamp.station_id,
    list_stations_timestamp.ingestion_time,
    IFNULL(nb_bikes_available_mechanic, 0) AS nb_bikes_available_mechanic,
    IFNULL(nb_bikes_available_electric, 0) AS nb_bikes_available_electric,
    IFNULL(nb_bikes_available_mechanic, 0) + IFNULL(nb_bikes_available_electric, 0) AS nb_bikes_available
  FROM list_stations_timestamp
  LEFT JOIN available_mechanic_bikes
  ON list_stations_timestamp.station_id = available_mechanic_bikes.station_id
  AND list_stations_timestamp.ingestion_time = available_mechanic_bikes.ingestion_time
  LEFT JOIN available_electric_bikes
  ON list_stations_timestamp.station_id = available_electric_bikes.station_id
  AND list_stations_timestamp.ingestion_time = available_electric_bikes.ingestion_time

)

SELECT 
  station_id,
  ingestion_time,
  nb_bikes_available_mechanic,
  nb_bikes_available_electric,
  stations_with_nb_available_bikes.nb_bikes_available
FROM stations_with_nb_available_bikes
ORDER BY station_id, ingestion_time DESC