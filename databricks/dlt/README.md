# Delta Live Tables (DLT) Implementation

This directory contains the Delta Live Tables implementation of the Music Streaming Analytics Platform data processing pipeline. DLT provides a simpler, more reliable way to build and maintain data pipelines with automatic data quality management.

## Pipeline Overview

The DLT pipeline follows the same medallion architecture as the original implementation:

1. **Raw Layer**: Initial ingestion from Kafka
2. **Bronze Layer**: Cleaned and validated data
3. **Silver Layer**: Business-level transformations
4. **Gold Layer**: Aggregated and business-ready data

## Implementation Structure

```
dlt/
├── src/
│   ├── raw/
│   │   └── raw_events.py        # Raw layer DLT pipeline
│   ├── bronze/
│   │   └── bronze_events.py     # Bronze layer DLT pipeline
│   ├── silver/
│   │   └── silver_events.py     # Silver layer DLT pipeline
│   └── gold/
│       └── gold_events.py       # Gold layer DLT pipeline
├── tests/
│   └── data_quality.py          # Data quality tests
└── config/
    └── pipeline_config.py       # Pipeline configuration
```

## Converting from Notebooks to DLT

### 1. Raw Layer (from 02_Ingest_From_Kafka_To_Raw.ipynb)

```python
import dlt
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, BooleanType

# Define schemas (reuse from original notebook)
base_schema = StructType([
    StructField("ts", LongType(), True),
    StructField("sessionId", StringType(), True),
    # ... other base fields ...
])

page_view_schema = StructType(base_schema.fields + [
    StructField("page", StringType(), True),
    StructField("method", StringType(), True),
    StructField("status", LongType(), True)
])

# ... other schemas ...

@dlt.table(
    name="raw_page_view_events",
    comment="Raw page view events from Kafka"
)
def raw_page_view_events():
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "172.212.37.12:9092")
        .option("subscribe", "page_view_events")
        .option("startingOffsets", "earliest")
        .load()
        .select(
            col("timestamp").alias("kafka_timestamp"),
            col("topic").alias("kafka_topic"),
            from_json(col("value").cast("string"), page_view_schema).alias("data")
        )
        .select("kafka_timestamp", "kafka_topic", "data.*")
    )
```

### 2. Bronze Layer (from 03_Process_From_Raw_To_Bronze.ipynb)

```python
@dlt.table(
    name="bronze_page_view_events",
    comment="Cleaned and validated page view events"
)
@dlt.expect("valid_timestamp", "ts IS NOT NULL")
@dlt.expect("valid_user_id", "userId IS NOT NULL")
def bronze_page_view_events():
    return (
        dlt.read("raw_page_view_events")
        .filter(col("ts").isNotNull())
        .filter(col("userId").isNotNull())
        .withColumn("event_timestamp", from_unixtime(col("ts")/1000))
    )
```

### 3. Silver Layer (from 04.1_Stream_Process_From_Bronze_To_Silver.ipynb)

```python
@dlt.table(
    name="silver_user_sessions",
    comment="User session aggregations"
)
def silver_user_sessions():
    return (
        dlt.read("bronze_page_view_events")
        .groupBy("sessionId")
        .agg(
            count("*").alias("page_views"),
            min("event_timestamp").alias("session_start"),
            max("event_timestamp").alias("session_end")
        )
    )
```

### 4. Gold Layer (from 05_Process_From_Silver_To_Gold.ipynb)

```python
@dlt.table(
    name="gold_daily_user_metrics",
    comment="Daily aggregated user metrics"
)
def gold_daily_user_metrics():
    return (
        dlt.read("silver_user_sessions")
        .withColumn("date", date(col("session_start")))
        .groupBy("date", "userId")
        .agg(
            count("sessionId").alias("total_sessions"),
            sum("page_views").alias("total_page_views")
        )
    )
```

## Key DLT Features Used

1. **Automatic Data Quality**
   ```python
   @dlt.expect("valid_timestamp", "ts IS NOT NULL")
   @dlt.expect_or_fail("valid_user_id", "userId IS NOT NULL")
   ```

2. **Change Data Capture**
   ```python
   @dlt.table(
       name="silver_user_profiles",
       comment="User profile information"
   )
   def silver_user_profiles():
       return (
           dlt.read("bronze_page_view_events")
           .select("userId", "firstName", "lastName", "gender")
           .distinct()
       )
   ```

3. **Materialized Views**
   ```python
   @dlt.view(
       name="active_users",
       comment="Currently active users"
   )
   def active_users():
       return (
           dlt.read("silver_user_sessions")
           .filter(col("session_end") > current_timestamp() - expr("INTERVAL 30 MINUTES"))
       )
   ```

## Pipeline Configuration

Create a pipeline configuration file:

```python
# config/pipeline_config.py
pipeline_config = {
    "name": "music_streaming_pipeline",
    "development": True,
    "continuous": True,
    "clusters": {
        "main": {
            "label": "default",
            "num_workers": 2
        }
    },
    "libraries": {
        "notebook": {
            "path": "src/raw/raw_events"
        }
    },
    "target": "music_streaming",
    "channel": "preview"
}
```

## Running the Pipeline

1. **Create Pipeline**
   ```bash
   databricks pipelines create --config config/pipeline_config.json
   ```

2. **Start Pipeline**
   ```bash
   databricks pipelines start --pipeline-id <pipeline-id>
   ```

3. **Monitor Pipeline**
   - Use Databricks UI to monitor pipeline health
   - Check data quality metrics
   - View pipeline logs

## Benefits of DLT Implementation

1. **Simplified Development**
   - Declarative pipeline definition
   - Automatic data quality management
   - Built-in monitoring and observability

2. **Improved Reliability**
   - Automatic retries
   - Data quality constraints
   - Change Data Capture support

3. **Better Performance**
   - Optimized Delta Lake operations
   - Efficient streaming processing
   - Automatic optimization

## Migration Steps

1. **Prepare Environment**
   - Create new DLT-enabled cluster
   - Set up required permissions
   - Configure storage locations

2. **Convert Code**
   - Transform notebook cells into DLT functions
   - Add data quality expectations
   - Implement proper error handling

3. **Test and Validate**
   - Run pipeline in development mode
   - Verify data quality
   - Compare results with original pipeline

4. **Deploy**
   - Create production pipeline
   - Configure monitoring
   - Set up alerts

## Best Practices

1. **Data Quality**
   - Define clear expectations
   - Use appropriate constraint types
   - Monitor quality metrics

2. **Performance**
   - Optimize table partitioning
   - Use appropriate cluster sizes
   - Monitor resource utilization

3. **Maintenance**
   - Regular pipeline health checks
   - Update data quality rules
   - Monitor storage usage

## Troubleshooting

1. **Common Issues**
   - Pipeline failures
   - Data quality violations
   - Performance problems

2. **Solutions**
   - Check pipeline logs
   - Review data quality metrics
   - Optimize resource allocation

## Contributing

When adding new features or modifications:
1. Create a new branch
2. Test changes in development pipeline
3. Update documentation
4. Submit for review

## License

This project is licensed under the MIT License - see the LICENSE file for details. 