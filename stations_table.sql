CREATE TABLE IF NOT EXISTS stations (
id INTEGER,
name TEXT,
city TEXT,
latitude NUMERIC,
longitudee NUMERIC,
dpcapacity INTEGER,
online_date TEXT,

CONSTRAINT stations_pk
    PRIMARY KEY (id))
