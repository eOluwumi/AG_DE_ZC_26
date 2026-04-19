# Analytics Engineering (NY Taxi Rides)

## Overview
This folder contains the analytics engineering layer of the data pipeline for the NYC Taxi dataset (yellow and green trip data).

The goal is to transform raw trip records stored in Azure SQL Database into clean, analytics-ready datasets using dbt.

## Purpose
- Build structured data models from raw taxi trip data
- Apply transformations using dbt (staging → fact → dimension models)
- Prepare datasets for downstream analytics and dashboarding

## Data Sources
- Yellow Taxi Trip Data
- Green Taxi Trip Data

Both datasets are ingested into Azure SQL Database via the ingestion pipeline in the main repository.

## Tools & Technologies
- dbt (data transformations)
- Azure SQL Database (data warehouse)
- Python (upstream ingestion)
- Streamlit (downstream visualization)

## Planned Models
- Staging models (`stg_yellow`, `stg_green`)
- Fact model (`fact_trips`)
- Dimension models (zones, categories, etc.)

## Workflow
1. Raw taxi data is ingested into Azure SQL Database
2. dbt models transform raw data into structured tables
3. Final datasets are used for analytics and dashboarding

## Notes
This layer is strictly focused on transformations and analytics logic. Ingestion and orchestration are handled in the main pipeline repository.