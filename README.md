# Motorsport Telemetry Analytics Platform (AWS + F1 Data)

A cloud-based motorsport telemetry analytics platform built on AWS. This project ingests Formula 1 race and telemetry data, stores it in a partitioned S3 data lake, exposes it through Athena, and powers BI dashboards in Power BI.

## What this project does

This repo packages an end-to-end pipeline that:

- pulls Formula 1 session data from FastF1 and Ergast
- runs the ingestion pipeline in a Docker container
- executes the container on AWS ECS Fargate
- writes raw and curated Parquet data to Amazon S3
- exposes SQL analytics through Amazon Athena
- supports Power BI reporting through Athena-compatible views

The project is intentionally AWS-forward and portfolio-oriented. It demonstrates a production-style batch analytics pattern rather than a notebook-only workflow.

## Architecture

```text
FastF1 / Ergast APIs
        │
        ▼
Python telemetry ingestion pipeline
        │
        ▼
Docker container
        │
        ▼
Amazon ECR
        │
        ▼
Amazon ECS Fargate task
        │
        ▼
Amazon S3 data lake (raw + curated Parquet)
        │
        ▼
Amazon Athena tables and views
        │
        ▼
Power BI dashboards
```

## AWS services used

- Amazon ECS Fargate
- Amazon ECR
- Amazon S3
- Amazon Athena
- Amazon CloudWatch Logs
- Amazon EventBridge
- AWS IAM

## Local stack

- Python 3.11
- Docker
- pandas
- PyArrow
- FastF1
- boto3
- Power BI Desktop

## Repository structure

```text
motorsport-telemetry-repo/
├── app/
│   ├── main.py
│   └── requirements.txt
├── ecs/
│   └── task-definition.template.json
├── events/
│   └── targets.template.json
├── iam/
│   ├── ecs-tasks-trust.json
│   ├── eventbridge-trust.json
│   ├── eventbridge-run-ecs-policy.json
│   └── task-s3-policy.template.json
├── powerbi/
│   └── native_queries.sql
├── sql/
│   ├── athena_tables.sql
│   └── powerbi_views.sql
├── Dockerfile
└── README.md
```

## Data flow

The ingestion container runs for a fixed set of 2024 sessions by default:

- Events: Bahrain, Monaco, Great Britain
- Sessions: Qualifying and Race
- Drivers: top 5 finishers by default for telemetry-heavy processing

### Raw layer

The pipeline writes source-aligned data to S3 under `raw/`.

Examples:

- `raw/fastf1/laps/season=2024/event=bahrain/session=Q/laps.parquet`
- `raw/fastf1/telemetry/season=2024/event=bahrain/session=R/driver=VER/telemetry.parquet`
- `raw/fastf1/meta/.../session_meta.json`

### Curated layer

The pipeline writes analytics-friendly data to `curated/`.

Examples:

- `curated/laps/season=2024/event=bahrain/session=R/laps.parquet`
- `curated/weather/season=2024/event=bahrain/session=R/weather.parquet`
- `curated/telemetry/season=2024/event=bahrain/session=R/driver=VER/telemetry.parquet`

## Core datasets

### curated_laps

One row per lap with race/session metadata.

Key fields:

- `season`
- `event`
- `session`
- `driver`
- `team`
- `lap_number`
- `stint`
- `compound`
- `tyre_life`
- `lap_time_seconds`
- `sector1_seconds`
- `sector2_seconds`
- `sector3_seconds`

### curated_telemetry

Time-series telemetry by driver and lap.

Key fields include:

- `driver`
- `lap_number`
- `ts_ms`
- `speed_kmh`
- `throttle`
- `brake`
- `gear`
- `rpm`
- `drs`
- `x`
- `y`
- `z`

## Mechanical engineering relevance

This project is data engineering first, but the telemetry supports several motorsport engineering concepts.

### Vehicle dynamics

Using speed, lap time, and track position, the dataset can support:

- acceleration and deceleration analysis
- braking zone comparisons
- cornering efficiency proxies
- lap pace and sector performance analysis

### Powertrain behavior

The telemetry includes:

- RPM
- gear
- throttle
- speed

This supports analyses like:

- gear shift behavior
- RPM usage across laps
- power delivery behavior by driver

### Tire and stint behavior

The laps dataset includes:

- compound
- tyre_life
- stint

This supports:

- tire degradation analysis
- stint performance comparisons
- compound-based pace comparison

### Aerodynamic proxies

This public telemetry does not contain enough information to calculate true drag or lift coefficients, but it can support aerodynamic efficiency proxies such as:

- top speed comparisons
- DRS usage patterns
- corner-speed versus straight-line-speed tradeoffs

## Prerequisites

Before deployment, make sure you have:

- an AWS account with permissions for ECS, ECR, S3, IAM, EventBridge, Athena, and CloudWatch
- AWS CLI configured
- Docker installed and running
- an S3 bucket for the data lake
- an S3 bucket/prefix for Athena results
- a VPC with two public subnets and a security group allowing outbound internet access

## Environment variables used by the container

The container reads these variables at runtime:

- `BUCKET`
- `SEASON`
- `EVENTS`
- `SESSIONS`
- `DRIVERS_MODE`
- `RAW_PREFIX`
- `CURATED_PREFIX`
- `FASTF1_CACHE_DIR`
- `MAX_RETRIES`
- `RETRY_BACKOFF_SECONDS`

## Build locally

Build the image:

```bash
docker build -t motorsport-telemetry-ingestor .
```

Tag and push to ECR:

```bash
docker tag motorsport-telemetry-ingestor:latest <ACCOUNT_ID>.dkr.ecr.us-east-1.amazonaws.com/motorsport-telemetry-ingestor:latest
docker push <ACCOUNT_ID>.dkr.ecr.us-east-1.amazonaws.com/motorsport-telemetry-ingestor:latest
```

## Deploy on AWS

### 1. Create IAM roles

Use the trust and policy templates in `iam/` to create:

- ECS execution role
- ECS task role
- EventBridge role

### 2. Register the ECS task definition

Use `ecs/task-definition.template.json` and replace:

- execution role ARN
- task role ARN
- image URI
- data bucket
- log group
- region
- task family

### 3. Run the task manually

Run a one-off ECS Fargate task for validation.

### 4. Schedule with EventBridge

Use `events/targets.template.json` and point it to:

- cluster ARN
- EventBridge role ARN
- task definition ARN
- public subnets
- security group

## Athena setup

The Athena DDL lives in `sql/athena_tables.sql`.

After replacing the bucket placeholder, run:

1. create database
2. create `curated_laps`
3. run `MSCK REPAIR TABLE`

## Power BI setup

Power BI worked most reliably against a flattened Athena view rather than the raw table.

The Power BI-facing Athena views live in `sql/powerbi_views.sql`.

Recommended sequence:

1. create the views in Athena
2. connect Power BI through Athena ODBC
3. use **Import** mode
4. use native SQL queries from `powerbi/native_queries.sql`

### Why the flattened Power BI views exist

Direct table/view browsing through ODBC can fail with Athena metadata edge cases. The flattened views:

- coerce types explicitly
- replace null-heavy fields with scalar defaults where needed
- simplify import into Power BI

## Example Athena query

Top 3 drivers by average Bahrain race pace:

```sql
SELECT
    driver,
    COUNT(*) AS total_laps,
    AVG(lap_time_seconds) AS avg_lap_time,
    MIN(lap_time_seconds) AS fastest_lap,
    MAX(lap_time_seconds) AS slowest_lap,
    AVG(sector1_seconds) AS avg_sector1,
    AVG(sector2_seconds) AS avg_sector2,
    AVG(sector3_seconds) AS avg_sector3
FROM motorsport_telemetry.curated_laps
WHERE season = 2024
  AND event = 'bahrain'
  AND session = 'R'
GROUP BY driver
ORDER BY avg_lap_time ASC
LIMIT 3;
```

## Example dashboards

This project is set up to support:

- driver performance summary
- sector comparison dashboard
- tire and stint performance dashboard
- lap trend analysis
- driver consistency dashboard

## Notes and known issues

- FastF1 may emit warnings about `X`, `Y`, and `Z` telemetry dtype preservation. These are usually non-fatal.
- Athena schema drift can occur if Parquet files are written with inconsistent types. The current pipeline normalizes critical numeric fields before writing.
- Power BI DirectQuery through Athena can be fragile. Import mode with Power BI-safe Athena views was the most reliable option.

## Future improvements

Possible extensions:

- telemetry-derived mechanical features (braking point, throttle smoothness, RPM band usage)
- strategy and tire degradation modeling
- additional circuits and sessions
- QuickSight or Streamlit dashboards
- CI/CD for infrastructure and deployment

## Author

Shrikar Chalam
