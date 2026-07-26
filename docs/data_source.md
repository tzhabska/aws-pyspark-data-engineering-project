# Data Source

## Dataset Name

NYC Taxi Trip Records

## Description

This project uses NYC Taxi Trip Records as a large-scale transactional dataset to demonstrate an end-to-end Data Engineering pipeline.

The dataset contains millions of taxi trip records with information about trip time, locations, distance, fares, and payment details.

## Why This Dataset

The dataset was selected because it provides realistic Data Engineering challenges:

- large volume of records;
- time-based partitioning opportunities;
- join operations with dimension tables;
- data quality validation scenarios;
- Spark performance optimization opportunities.

## Data Format

Source format:

- CSV / Parquet

Target format:

- Parquet

Parquet is used as an optimized columnar storage format for analytical workloads.

## Main Dataset Schema

### Trip Data (Fact)

| Column | Description |
|---|---|
| trip_id | Unique trip identifier |
| pickup_datetime | Trip start timestamp |
| dropoff_datetime | Trip end timestamp |
| pickup_location_id | Pickup location identifier |
| dropoff_location_id | Dropoff location identifier |
| passenger_count | Number of passengers |
| trip_distance | Distance traveled |
| fare_amount | Trip fare |
| payment_type | Payment method |

## Dimension Data

### Location Data

| Column | Description |
|---|---|
| location_id | Location identifier |
| borough | NYC borough |
| zone | Taxi zone |
| service_zone | Service area |

## Data Processing Goals

The pipeline will demonstrate:

- ingestion into AWS S3;
- PySpark transformation;
- data quality checks;
- partitioning strategies;
- join optimization;
- data skew analysis;
- analytical data modeling.
