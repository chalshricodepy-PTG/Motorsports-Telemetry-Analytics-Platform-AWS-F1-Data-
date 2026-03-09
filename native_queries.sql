-- Import these one at a time in Power BI via ODBC Advanced Options.

SELECT *
FROM motorsport_telemetry.v_pbi_laps_flat
WHERE season = 2024;

SELECT *
FROM motorsport_telemetry.v_pbi_driver_summary_flat
WHERE season = 2024;

SELECT *
FROM motorsport_telemetry.v_pbi_tire_performance_flat
WHERE season = 2024;
