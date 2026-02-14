# YouTube Analytics dbt Pipeline

A dbt project for analyzing YouTube trending video data in Snowflake.

## Project Structure

```
youtube_analytics/
├── models/
│   ├── staging/          # Raw data transformations
│   ├── intermediate/     # Enriched video data
│   └── marts/           # Analytics tables
├── tests/               # Data quality tests
└── dbt_project.yml
```

## Models

**Staging:**
- `stg_youtube_videos` - Cleaned video data
- `stg_categories` - Category reference data

**Intermediate:**
- `int_videos_enriched` - Videos with category details

**Marts:**
- `fct_trending_videos` - Fact table of trending videos
- `dim_categories` - Category dimension
- `dim_channels` - Channel dimension
- `mart_category_performance` - Category metrics
- `mart_channel_rankings` - Top channels by engagement
- `mart_daily_trends` - Daily trending patterns
- `mart_viral_analysis` - Viral video analysis

## Setup

1. Configure dbt profile in `~/.dbt/profiles.yml`
2. Install dependencies: `dbt deps`
3. Run models: `dbt run`
4. Run tests: `dbt test`

## Airflow Automation

The pipeline can be automated using Airflow:

```bash
# Copy DAG to Airflow
cp dags/youtube_analytics_dag.py $AIRFLOW_HOME/dags/

# DAG runs daily and executes:
# 1. dbt run (build all models)
# 2. dbt test (validate data quality)
```

Trigger manually in Airflow UI or wait for scheduled run.
