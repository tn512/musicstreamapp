# Ingesting Streaming Data from Kafka to Azure Data Lake using Databricks with Unity Catalog

This guide provides step-by-step instructions for setting up a streaming pipeline from Kafka to Azure Data Lake Storage Gen2 using Databricks with Unity Catalog.

## 1. Prerequisites

Before you begin, ensure you have:
- Kafka cluster running with topics containing your event data
- Azure Data Lake Storage Gen2 account created
- Databricks workspace set up in Azure with Unity Catalog enabled
- A metastore, catalog, and schema created in Unity Catalog
- Service principal or managed identity with proper access to your storage account

## 2. Set Up Unity Catalog Resources

First, ensure you have the necessary Unity Catalog resources:

```sql
-- Create a catalog if it doesn't exist
CREATE CATALOG IF NOT EXISTS music_streaming;

-- Use the catalog
USE CATALOG music_streaming;

-- Create a schema for raw data
CREATE SCHEMA IF NOT EXISTS raw;

-- Create a schema for processed data
CREATE SCHEMA IF NOT EXISTS processed;
```

## 3. Configure Databricks Access to ADLS Gen2

With Unity Catalog, you'll typically use storage credentials:

```sql
-- Create storage credential (run this in SQL workspace as a metastore admin)
CREATE STORAGE CREDENTIAL IF NOT EXISTS azure_storage_credential
  WITH AZURE MANAGED IDENTITY
  COMMENT 'Credential for accessing our ADLS storage';

-- Alternatively, use service principal
CREATE STORAGE CREDENTIAL IF NOT EXISTS azure_storage_credential
  WITH AZURE SERVICE PRINCIPAL
  AZURE_CLIENT_ID = '<application-id>'
  AZURE_CLIENT_SECRET = '<service-principal-secret>'
  AZURE_TENANT_ID = '<tenant-id>'
  COMMENT 'Credential for accessing our ADLS storage';
```

## 4. Create External Location

```sql
-- Create external location (run this in SQL workspace as a metastore admin)
CREATE EXTERNAL LOCATION IF NOT EXISTS music_streaming_data
  URL 'abfss://<container>@<storage-account-name>.dfs.core.windows.net/'
  WITH (STORAGE CREDENTIAL azure_storage_credential)
  COMMENT 'External location for music streaming data';
```

## 5. Create a Databricks Notebook for Kafka Streaming

### Step 1: Set up Kafka Connection Parameters
```python
# Kafka connection parameters
kafka_bootstrap_servers = "<your-kafka-external-ip>:9092"
kafka_topic = "page_view_events"  # Or any other topic you want to consume

# Unity Catalog paths
catalog_name = "music_streaming"
schema_name = "raw"
table_name = "page_view_events"

# Checkpoint location in ADLS Gen2 (using external location)
checkpoint_location = "abfss://<container>@<storage-account-name>.dfs.core.windows.net/checkpoints/kafka_stream"
```

### Step 2: Create Streaming DataFrame from Kafka
```python
# Read stream from Kafka
kafka_stream_df = (spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
  .option("subscribe", kafka_topic)
  .option("startingOffsets", "earliest")  # Use "latest" for production
  .option("failOnDataLoss", "false")
  .load()
)

# Display schema of the Kafka stream
kafka_stream_df.printSchema()
```

### Step 3: Parse the Kafka Messages
```python
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, TimestampType

# Define schema based on your event data structure
# This is an example schema for page_view_events - adjust according to your actual data
schema = StructType([
    StructField("ts", LongType(), True),
    StructField("sessionId", StringType(), True),
    StructField("userId", StringType(), True),
    StructField("auth", StringType(), True),
    StructField("level", StringType(), True),
    StructField("itemInSession", LongType(), True),
    StructField("page", StringType(), True),
    StructField("method", StringType(), True),
    StructField("status", LongType(), True),
    StructField("city", StringType(), True),
    StructField("zip", StringType(), True),
    StructField("state", StringType(), True),
    StructField("userAgent", StringType(), True),
    StructField("lon", DoubleType(), True),
    StructField("lat", DoubleType(), True),
    StructField("firstName", StringType(), True),
    StructField("lastName", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("registration", LongType(), True)
])

# Parse the value column from Kafka which contains the JSON data
parsed_df = kafka_stream_df.select(
    col("timestamp").alias("kafka_timestamp"),
    col("topic").alias("kafka_topic"),
    col("partition").alias("kafka_partition"),
    col("offset").alias("kafka_offset"),
    from_json(col("value").cast("string"), schema).alias("data")
).select(
    "kafka_timestamp", 
    "kafka_topic", 
    "kafka_partition", 
    "kafka_offset", 
    "data.*"
)

# Add ingestion timestamp
parsed_df = parsed_df.withColumn("ingestion_time", current_timestamp())

# Display the parsed schema
parsed_df.printSchema()
```

### Step 4: Write Stream to Unity Catalog Table
```python
# Full table path in Unity Catalog
table_path = f"{catalog_name}.{schema_name}.{table_name}"

# Write the stream to Delta table in Unity Catalog
stream_query = (parsed_df.writeStream
  .format("delta")
  .outputMode("append")
  .option("checkpointLocation", checkpoint_location)
  .partitionBy("page")  # Optional: partition by a field that makes sense for your data
  .trigger(processingTime="2 minutes")  # Process every 2 minutes
  .option("mergeSchema", "true")  # Allow schema evolution
  .toTable(table_path)
)

# Wait for the streaming query to terminate
stream_query.awaitTermination()
```

## 6. Create a Volume for Checkpoints (Optional but Recommended)

For better management of checkpoints, you can create a volume in Unity Catalog:

```sql
-- Create a volume for checkpoints
CREATE VOLUME IF NOT EXISTS music_streaming.raw.kafka_checkpoints;
```

Then update your checkpoint location:

```python
# Checkpoint location using Unity Catalog volume
checkpoint_location = "volume://music_streaming.raw.kafka_checkpoints/page_view_events"
```

## 7. Schedule the Notebook as a Job

1. In Databricks, go to "Workflows" in the sidebar
2. Click "Create Job"
3. Add your notebook as a task
4. Configure the job to run on a schedule or continuously
5. Select an appropriate cluster configuration with Unity Catalog enabled
6. Save and run the job

## 8. Set Up Access Control with Unity Catalog

```sql
-- Grant permissions to specific users or groups
GRANT SELECT ON TABLE music_streaming.raw.page_view_events TO `analysts`;
GRANT MODIFY ON TABLE music_streaming.raw.page_view_events TO `data_engineers`;

-- For volumes
GRANT READ ON VOLUME music_streaming.raw.kafka_checkpoints TO `data_engineers`;
GRANT WRITE ON VOLUME music_streaming.raw.kafka_checkpoints TO `data_engineers`;
```

## 9. Monitor Your Streaming Job

- Check the Spark UI for streaming statistics
- Monitor the table in Unity Catalog
- Set up alerts for job failures

## 10. Best Practices with Unity Catalog

1. **Data Governance**: Use table properties and comments for documentation
   ```sql
   COMMENT ON TABLE music_streaming.raw.page_view_events IS 'Raw page view events from music streaming service';
   ```

2. **Data Lineage**: Unity Catalog automatically tracks lineage

3. **Security**: Use row-level and column-level security for sensitive data
   ```sql
   -- Example of column-level security
   GRANT SELECT ON TABLE music_streaming.raw.page_view_events (ts, page, method, status) TO `analysts`;
   ```

4. **Data Quality**: Implement quality checks using expectations or constraints

5. **Versioning**: Use table versions for point-in-time recovery
   ```sql
   -- Create a version
   CREATE VERSION OF TABLE music_streaming.raw.page_view_events AS OF VERSION AS OF TIMESTAMP '<timestamp>';
   ```

## 11. Handling Multiple Kafka Topics

If you have multiple event types in different Kafka topics (e.g., page_view_events, listen_events, auth_events), you can:

1. Create separate streaming jobs for each topic
2. Create separate tables for each event type
3. Use the same pattern for each with appropriate schema adjustments

Example for listen_events:

```python
# Adjust schema for listen_events
listen_events_schema = StructType([
    StructField("ts", LongType(), True),
    StructField("sessionId", StringType(), True),
    StructField("userId", StringType(), True),
    # ... other fields specific to listen events
    StructField("artist", StringType(), True),
    StructField("song", StringType(), True),
    StructField("duration", LongType(), True)
])

# Create streaming job for listen_events
listen_events_df = (spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
  .option("subscribe", "listen_events")  # Different topic
  .option("startingOffsets", "earliest")
  .option("failOnDataLoss", "false")
  .load()
)

# Parse and process similarly to page_view_events
# ...

# Write to a different table
listen_table_path = f"{catalog_name}.{schema_name}.listen_events"
```

## 12. Troubleshooting

### Common Issues and Solutions

1. **Connection Issues to Kafka**:
   - Verify network connectivity between Databricks and Kafka
   - Check security settings (TLS, authentication)
   - Ensure the Kafka bootstrap servers are accessible from Databricks

2. **Schema Evolution Errors**:
   - Use `mergeSchema` option
   - Consider using a more flexible schema approach

3. **Checkpoint Issues**:
   - Ensure the checkpoint location is accessible and writable
   - If restarting a failed job with a new schema, you may need to delete the checkpoint

4. **Performance Issues**:
   - Adjust trigger interval
   - Optimize cluster configuration
   - Consider partitioning strategy

### Debugging Tips

```python
# For debugging, you can run a small batch to test
test_df = (spark.read
  .format("kafka")
  .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
  .option("subscribe", kafka_topic)
  .option("startingOffsets", "earliest")
  .option("endingOffsets", "latest")
  .load()
  .limit(10)
)

# Display raw messages
display(test_df.select(col("value").cast("string")))
``` 