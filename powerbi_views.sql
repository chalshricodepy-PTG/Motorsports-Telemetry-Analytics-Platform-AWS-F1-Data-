CREATE OR REPLACE VIEW motorsport_telemetry.v_pbi_laps_flat AS
SELECT
    CAST(COALESCE(season, 0) AS INTEGER) AS season,
    CAST(COALESCE(event, '') AS VARCHAR) AS event,
    CAST(COALESCE(session, '') AS VARCHAR) AS session,
    CAST(COALESCE(driver, '') AS VARCHAR) AS driver,
    CAST(COALESCE(team, '') AS VARCHAR) AS team,
    CAST(COALESCE(lap_number, 0) AS BIGINT) AS lap_number,
    CAST(COALESCE(stint, 0) AS BIGINT) AS stint,
    CAST(COALESCE(compound, '') AS VARCHAR) AS compound,
    CAST(COALESCE(tyre_life, 0) AS BIGINT) AS tyre_life,
    CAST(COALESCE(track_status, '') AS VARCHAR) AS track_status,
    CAST(COALESCE(pit_in_time, '') AS VARCHAR) AS pit_in_time,
    CAST(COALESCE(pit_out_time, '') AS VARCHAR) AS pit_out_time,
    CAST(COALESCE(lap_time_seconds, 0.0) AS DOUBLE) AS lap_time_seconds,
    CAST(COALESCE(sector1_seconds, 0.0) AS DOUBLE) AS sector1_seconds,
    CAST(COALESCE(sector2_seconds, 0.0) AS DOUBLE) AS sector2_seconds,
    CAST(COALESCE(sector3_seconds, 0.0) AS DOUBLE) AS sector3_seconds,
    CAST(COALESCE(CAST(is_personal_best AS INTEGER), 0) AS INTEGER) AS is_personal_best,
    CAST(COALESCE(CAST(is_accurate AS INTEGER), 0) AS INTEGER) AS is_accurate
FROM motorsport_telemetry.curated_laps;

CREATE OR REPLACE VIEW motorsport_telemetry.v_pbi_driver_summary_flat AS
SELECT
    season,
    event,
    session,
    driver,
    team,
    COUNT(*) AS total_laps,
    AVG(lap_time_seconds) AS avg_lap_time,
    MIN(lap_time_seconds) AS fastest_lap,
    MAX(lap_time_seconds) AS slowest_lap,
    AVG(sector1_seconds) AS avg_sector1,
    AVG(sector2_seconds) AS avg_sector2,
    AVG(sector3_seconds) AS avg_sector3
FROM motorsport_telemetry.v_pbi_laps_flat
GROUP BY season, event, session, driver, team;

CREATE OR REPLACE VIEW motorsport_telemetry.v_pbi_tire_performance_flat AS
SELECT
    season,
    event,
    session,
    driver,
    team,
    compound,
    stint,
    COUNT(*) AS laps_in_stint,
    AVG(lap_time_seconds) AS avg_lap_time,
    MIN(lap_time_seconds) AS fastest_lap,
    MAX(lap_time_seconds) AS slowest_lap,
    AVG(tyre_life) AS avg_tyre_life
FROM motorsport_telemetry.v_pbi_laps_flat
GROUP BY season, event, session, driver, team, compound, stint;
