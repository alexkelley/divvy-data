CREATE TABLE trips (
       trip_id INTEGER,
       start_time TEXT,
       end_time TEXT,
       bikeid INTEGER,
       tripduration INTEGER,
       from_station_id INTEGER,
       from_station_name TEXT,
       to_station_id INTEGER,
       to_station_name TEXT,
       usertype TEXT,
       gender TEXT,
       birthyear INTEGER,

CONSTRAINT trips_pk
    PRIMARY KEY (trip_id),

CONSTRAINT from_station_fk
    FOREIGN KEY (from_station_id)
        REFERENCES stations(id),

CONSTRAINT to_station_fk
    FOREIGN KEY (to_station_id)
        REFERENCES stations(id));
