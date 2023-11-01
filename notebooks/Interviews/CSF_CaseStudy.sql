-- Pull station ID and name for join

WITH station AS (
    SELECT 
        name, 
        id AS station_id
    FROM stations
),

-- Join trip info to station table to get needed origin/destination info; ranked by trip distance from beginning station

trip_information AS (
    SELECT 
        trips.from_station_id AS origin_id,
        from_station.name AS origin_station,
        trips.to_station_id AS destination_id,
        to_station.name AS destination_station,
        trips.start_time_ts,
        trips.end_time_ts,
        tripduration / 60 AS trip_duration_minutes,
        ROW_NUMBER() OVER (PARTITION BY from_station_id ORDER BY tripduration DESC) AS duration_rank
    FROM trips
    LEFT JOIN station AS to_station ON to_station.station_id = trips.to_station_id
    LEFT JOIN station AS from_station ON from_station.station_id = trips.from_station_id
),

-- Output table; Grab the 2nd longest trip, accounting for the edge case of stations with less than two total trips

output AS (
    SELECT 
        origin_station,
        destination_station,
        trip_duration_minutes
        --, duration_rank
    FROM trip_information
    WHERE 
        duration_rank = 2
        OR (
            origin_id IN (
                SELECT origin_id 
                FROM trip_information
                GROUP BY origin_id
                HAVING COUNT(origin_id) < 2
            )
            AND duration_rank = 1
        )
),

-- Test for accuracy

test AS (
    SELECT * 
    FROM trip_information
    WHERE origin_station IN ('LaSalle (Wells) St & Huron St', 'State St & Randolph St', 'Canal St & Harrison St')
)

-- Select the output or test

SELECT *
FROM output;