# Music Streaming Analytics dbt Project

This dbt project implements the transformation layer for the Music Streaming Analytics Platform, following a medallion architecture:

- **Bronze Layer**: Cleaned and validated raw data from Kafka
- **Silver Layer**: Business-level transformations including dimension tables
- **Gold Layer**: Aggregated analytics-ready data

## Getting Started

### Prerequisites

- dbt CLI installed
- Databricks workspace with Unity Catalog
- Access to music_streaming catalog

### Setup

1. Clone this repository
2. Configure Databricks connection by setting environment variables:
   ```
   export DATABRICKS_HOST=your_workspace_url
   export DATABRICKS_HTTP_PATH=your_http_path
   export DATABRICKS_TOKEN=your_personal_access_token
   ```
3. Install dbt packages:
   ```
   dbt deps
   ```

## Project Structure

```
├── analyses/             # Ad-hoc analytical queries
├── macros/              # Reusable SQL functions
├── models/              # Core transformation models
│   ├── bronze/          # Initial cleaned data
│   ├── silver/          # Dimensional models and facts
│   └── gold/            # Analytics-ready tables
├── seeds/               # Static reference data
├── snapshots/           # Historical tracking (SCD) 
├── tests/               # Data tests
├── dbt_project.yml      # Project configuration
└── profiles.yml         # Connection profiles
```

## Key Models

### Bronze Layer
- **bronze_listen_events**: Cleaned listen events
- **bronze_page_view_events**: Cleaned page view events
- **bronze_auth_events**: Cleaned authentication events
- **bronze_status_change_events**: Cleaned status change events

### Silver Layer

#### Dimension Tables
- **dim_users**: User dimension table with SCD Type 2
- **dim_location**: Geographic locations
- **dim_songs**: Song metadata
- **dim_artists**: Artist information
- **dim_datetime**: Date and time dimension

#### Fact Tables
- **fact_listen_events**: Listen events facts

### Gold Layer
- **fact_stream**: Core fact table for analytics
- **daily_user_summary**: User-level daily aggregates
- **daily_location_summary**: Location-level daily aggregates
- **artist_popularity**: Artist popularity metrics

## Running the Project

### Full Refresh
```
dbt run
```

### Run Specific Models
```
dbt run --select bronze
dbt run --select silver
dbt run --select gold
```

### Run Tests
```
dbt test
```

### Generate Documentation
```
dbt docs generate
dbt docs serve
```

## Maintenance

### Updating Dependencies
```
dbt deps
```

### Debugging
```
dbt compile
```

## Contributing

1. Create a new branch
2. Make your changes
3. Run tests
4. Submit a pull request 