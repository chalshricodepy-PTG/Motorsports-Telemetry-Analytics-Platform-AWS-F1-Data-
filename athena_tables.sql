CREATE DATABASE IF NOT EXISTS motorsport_telemetry;

DROP TABLE IF EXISTS motorsport_telemetry.curated_laps;

CREATE EXTERNAL TABLE motorsport_telemetry.curated_laps (
  driver string,
  team string,
  lap_number bigint,
  stint bigint,
  compound string,
  tyre_life bigint,
  is_personal_best boolean,
  is_accurate boolean,
  track_status string,
  pit_in_time string,
  pit_out_time string,
  lap_time_seconds double,
  sector1_seconds double,
  sector2_seconds double,
  sector3_seconds double
)
PARTITIONED BY (
  season int,
  event string,
  session string
)
STORED AS PARQUET
LOCATION 's3://REPLACE_DATA_BUCKET/curated/laps/';

MSCK REPAIR TABLE motorsport_telemetry.curated_laps;
