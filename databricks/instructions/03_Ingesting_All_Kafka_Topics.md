# Ingesting Data from All Kafka Topics

This guide demonstrates how to ingest data from all four Kafka topics into Unity Catalog tables:
1. page_view_events_new
2. listen_events_new
3. auth_events_new
4. status_change_events_new

## Setup

First, run the initialization notebook:
```python
%run "/Users/drmaiatauros@hotmail.com/01_Initialize_Setting"
```

## Import Required Libraries

```python
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, TimestampType, BooleanType
```

## Configuration

```python
# Kafka connection parameters
kafka_bootstrap_servers = "4.246.237.185:9092"

# Unity Catalog paths
catalog_name = "music_streaming"
schema_name = "raw"

# Define topics and their corresponding table names
topics_config = {
    "page_view_events_new": "page_view_events",
    "listen_events_new": "listen_events",
    "auth_events_new": "auth_events",
    "status_change_events_new": "status_change_events"
}
```

## Define Schemas

```python
# Define schemas for each event type
base_schema = StructType([
    StructField("ts", LongType(), True),
    StructField("sessionId", StringType(), True),
    StructField("userId", StringType(), True),
    StructField("auth", StringType(), True),
    StructField("level", StringType(), True),
    StructField("itemInSession", LongType(), True),
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

page_view_schema = StructType(base_schema.fields + [
    StructField("page", StringType(), True),
    StructField("method", StringType(), True),
    StructField("status", LongType(), True)
])

listen_schema = StructType(base_schema.fields + [
    StructField("artist", StringType(), True),
    StructField("song", StringType(), True),
    StructField("duration", LongType(), True)
])

auth_schema = StructType(base_schema.fields + [
    StructField("success", BooleanType(), True),
    StructField("method", StringType(), True),
    StructField("status", LongType(), True)
])

status_change_schema = StructType(base_schema.fields + [
    StructField("prevLevel", StringType(), True),
    StructField("method", StringType(), True),
    StructField("status", LongType(), True),
    StructField("statusChangeType", StringType(), True)
])

# Map topics to their schemas
schemas = {
    "page_view_events_new": page_view_schema,
    "listen_events_new": listen_schema,
    "auth_events_new": auth_schema,
    "status_change_events_new": status_change_schema
}
```

## Helper Functions

```python
def create_kafka_stream(topic, checkpoint_path):
    """Create a Kafka stream for a given topic with settings optimized for batch jobs."""
    # Check if checkpoint exists
    try:
        dbutils.fs.ls(checkpoint_path)
        checkpoint_exists = True
    except:
        checkpoint_exists = False
    
    # Set starting offset based on checkpoint existence
    # For first run: use "earliest" to get all historical data
    # For subsequent runs: rely on checkpoint (no need to specify startingOffsets)
    kafka_options = {
        "kafka.bootstrap.servers": kafka_bootstrap_servers,
        "subscribe": topic,
        "failOnDataLoss": "false",
        "kafka.security.protocol": "PLAINTEXT"
    }
    
    # Only add startingOffsets for first run
    if not checkpoint_exists:
        kafka_options["startingOffsets"] = "earliest"
    
    # Create and return the stream
    return (spark.readStream
        .format("kafka")
        .options(**kafka_options)
        .load()
    )

def process_stream(topic, schema, table_name):
    """Process a Kafka stream and write to Unity Catalog table."""
    # Define checkpoint path
    checkpoint_path = f"/tmp/checkpoints/{table_name}"
    
    # Create Kafka stream
    kafka_stream_df = create_kafka_stream(topic, checkpoint_path)
    
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
    
    # Write to Unity Catalog table
    query = (parsed_df.writeStream
        .format("delta")
        .outputMode("append")
        .trigger(availableNow=True)  # Process all available data and then stop
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .toTable(f"{catalog_name}.{schema_name}.{table_name}")
    )
    
    return query
```

## Start Stream Processing

```python
# Start processing all streams
queries = []

for topic, table_name in topics_config.items():
    print(f"Starting stream processing for {topic}...")
    schema = schemas[topic]
    query = process_stream(topic, schema, table_name)
    queries.append(query)
    print(f"Stream processing started for {topic}")
```

## Monitor Stream Processing

```python
# Monitor each query
for i, (topic, table_name) in enumerate(topics_config.items()):
    print(f"\nStatus for {topic}:")
    print(queries[i].status)
```

## Query Ingested Data

```python
# Example queries for each table
for topic, table_name in topics_config.items():
    print(f"\nSample data from {table_name}:")
    display(spark.sql(f"SELECT * FROM {catalog_name}.{schema_name}.{table_name} LIMIT 5"))
```

## Stop Streams

When you're done, you can stop all streams:

```python
# Stop all streams
for i, (topic, table_name) in enumerate(topics_config.items()):
    print(f"Stopping stream for {topic}...")
    queries[i].stop()
    print(f"Stream stopped for {topic}")
```

## Notes

1. The schemas are designed based on the event generator code, ensuring all fields are properly captured.
2. Each stream is processed independently and written to its respective table.
3. The Kafka stream configuration is simplified for basic connectivity. If you encounter issues with:
   - Connection timeouts
   - Kafka broker connectivity problems
   - Data loss during processing
   - Performance degradation with high volume
   You may need to add additional Kafka configurations.
4. Using `trigger(availableNow=True)` makes this ideal for scheduled jobs, as it:
   - Processes all available data when the job runs
   - Continues processing new data that arrives during the job execution
   - Stops automatically when there's a pause in data arrival
   - Uses checkpoints to track progress between job runs
5. Intelligent checkpoint handling:
   - First run: Reads from earliest offset to process all historical data
   - Subsequent runs: Uses checkpoint to continue from last processed position
   - Prevents both data loss and performance problems with large historical datasets
6. Schema evolution is handled through the `mergeSchema` option.
7. The code includes monitoring capabilities to check stream status.

## Tables Created

The following tables will be created in your Unity Catalog:
- `music_streaming.raw.page_view_events`
- `music_streaming.raw.listen_events`
- `music_streaming.raw.auth_events`
- `music_streaming.raw.status_change_events`