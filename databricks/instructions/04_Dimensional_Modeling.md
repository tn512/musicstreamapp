# Dimensional Modeling for Music Streaming Data in Databricks

This guide demonstrates how to implement dimensional modeling in Databricks to transform raw music streaming data into analytics-ready tables using the Medallion Architecture with streaming processes.

## Medallion Architecture Overview

We'll organize our data transformation into three distinct layers:

1. **Bronze Layer** - Cleaned and validated raw data
   - Parsed data from Kafka with proper data types
   - Minimal transformations, focus on quality and consistency
   - Delta tables for each event type

2. **Silver Layer** - Dimension tables & normalized data
   - `dim_users` - User profiles
   - `dim_songs` - Song details
   - `dim_artists` - Artist information
   - `dim_datetime` - Time dimensions
   - `dim_location` - Geographic information

3. **Gold Layer** - Analytics-ready datasets
   - `fact_streams` - Core fact table with foreign keys
   - `wide_streams` - Denormalized view for analytics

## Implementation Steps

### Step 1: Set Up Your Notebook

```python
# Run initialization script if needed
%run "/Users/drmaiatauros@hotmail.com/01_Initialize_Setting"

# Import required libraries
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
import hashlib
```

### Step 2: Create Bronze Layer Tables with Streaming

Transform raw Kafka data into cleaned Bronze tables using streaming:

```python
# Function to create Bronze tables from raw data with streaming
def create_bronze_layer_streaming():
    # Create Bronze table for listen events using streaming
    listen_events_stream = (spark.readStream.table("music_streaming.raw.listen_events")
        .withColumn("timestamp", F.col("ts").cast("timestamp"))
        .withColumn("registration_time", F.col("registration").cast("timestamp"))
        .withColumn("latitude", F.col("lat"))
        .withColumn("longitude", F.col("lon"))
        .withColumn("ingestion_date", F.current_date())
        .withColumn("bronze_id", F.expr("uuid()"))
    )
    
    # Write to Bronze layer using streaming
    listen_query = (listen_events_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", "/tmp/delta/_checkpoints/bronze_listen_events")
        .partitionBy("ingestion_date")
        .toTable("music_streaming.bronze.listen_events"))
    
    # Create Bronze table for page view events using streaming
    page_view_events_stream = (spark.readStream.table("music_streaming.raw.page_view_events")
        .withColumn("timestamp", F.col("ts").cast("timestamp"))
        .withColumn("registration_time", F.col("registration").cast("timestamp"))
        .withColumn("latitude", F.col("lat"))
        .withColumn("longitude", F.col("lon"))
        .withColumn("ingestion_date", F.current_date())
        .withColumn("bronze_id", F.expr("uuid()"))
    )
    
    # Write to Bronze layer using streaming
    page_view_query = (page_view_events_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", "/tmp/delta/_checkpoints/bronze_page_view_events")
        .partitionBy("ingestion_date")
        .toTable("music_streaming.bronze.page_view_events"))
    
    # Return the streaming queries so they can be managed
    return {
        "listen_query": listen_query,
        "page_view_query": page_view_query
    }

# Execute the function to start streaming
streaming_queries = create_bronze_layer_streaming()

# You can access and manage the queries
# streaming_queries["listen_query"].awaitTermination()
# To stop: streaming_queries["listen_query"].stop()
```

### Step 3: Create Silver Layer (Dimension Tables) with Streaming Updates

Transform Bronze data into Silver layer dimension tables with streaming:

```python
# Function to create Silver layer dimension tables with streaming
def create_silver_layer_dimensions_streaming():
    # MD5 hash function for creating surrogate keys
    def md5_hash(*cols):
        return F.md5(F.concat_ws("|", *cols))
    
    # Dimension: Users - SCD Type 2 implementation for level changes
    # Use foreachBatch to implement SCD Type 2 logic for the users dimension
    users_stream = (spark.readStream.table("music_streaming.bronze.listen_events")
        .select("userId", "firstName", "lastName", "gender", "level", "registration_time", "timestamp")
        .filter(F.col("userId").isNotNull())
        .withColumn("firstName", F.coalesce(F.col("firstName"), F.lit("Unknown")))
        .withColumn("lastName", F.coalesce(F.col("lastName"), F.lit("Unknown")))
        .withColumn("gender", F.coalesce(F.col("gender"), F.lit("Unknown")))
    )
    
    # Process users with SCD Type 2 handling for level changes
    def process_users_scd2(batch_df, batch_id):
        if batch_df.isEmpty():
            return
            
        # Create a temporary view of the incoming batch
        batch_df.createOrReplaceTempView("users_batch")
        
        # Check if dim_users table exists
        tables = spark.sql("SHOW TABLES IN music_streaming.silver").filter(F.col("tableName") == "dim_users").collect()
        if len(tables) == 0:
            # First-time table creation that handles historical changes
            # This implementation captures the history of changes already in the source data
            
            # Create a temporary view of the current batch - this is redundant and causing the issue
            # We already created users_batch above, no need to create current_users_batch here
            
            spark.sql("""
                CREATE OR REPLACE TEMPORARY VIEW users_with_change_history AS
                WITH user_level_changes AS (
                    -- Detect level changes by comparing with previous level
                    SELECT 
                        userId, 
                        firstName,
                        lastName,
                        gender,
                        level,
                        registration_time,
                        timestamp,
                        LAG(level) OVER (PARTITION BY userId ORDER BY timestamp) AS prev_level,
                        -- Flag rows where level changed or it's the first appearance of the user
                        CASE 
                            WHEN LAG(level) OVER (PARTITION BY userId ORDER BY timestamp) IS NULL THEN 1
                            WHEN level != LAG(level) OVER (PARTITION BY userId ORDER BY timestamp) THEN 1
                            ELSE 0
                        END AS level_changed
                    FROM users_batch
                ),
                change_groups AS (
                    -- Assign a group number to each sequence of the same level
                    SELECT 
                        *,
                        SUM(level_changed) OVER (PARTITION BY userId ORDER BY timestamp) AS change_group
                    FROM user_level_changes
                ),
                change_boundaries AS (
                    -- Get the first and last timestamp for each level period
                    SELECT 
                        userId,
                        firstName,
                        lastName,
                        gender,
                        level,
                        registration_time,
                        change_group,
                        MIN(timestamp) AS start_date,
                        MAX(timestamp) AS end_date
                    FROM change_groups
                    GROUP BY userId, firstName, lastName, gender, level, registration_time, change_group
                ),
                finalized_changes AS (
                    -- Create proper activation and expiration dates for each record
                    SELECT 
                        userId,
                        firstName,
                        lastName,
                        gender,
                        level,
                        registration_time,
                        CAST(date_trunc('day', start_date) AS DATE) AS row_activation_date,
                        CASE 
                            WHEN LEAD(start_date) OVER (PARTITION BY userId ORDER BY start_date) IS NOT NULL 
                            THEN CAST(date_trunc('day', LEAD(start_date) OVER (PARTITION BY userId ORDER BY start_date)) AS DATE)
                            ELSE CAST('9999-12-31' AS DATE)
                        END AS row_expiration_date,
                        CASE 
                            WHEN LEAD(start_date) OVER (PARTITION BY userId ORDER BY start_date) IS NULL THEN 1
                            ELSE 0
                        END AS current_row,
                        md5(concat_ws('|', userId, level, start_date)) AS userKey
                    FROM change_boundaries
                )
                SELECT * FROM finalized_changes
                ORDER BY userId, row_activation_date
            """)
            
            # Write the SCD2 data to the dimension table
            (spark.table("users_with_change_history").write
                .format("delta")
                .mode("overwrite")
                .saveAsTable("music_streaming.silver.dim_users"))
                
            print("Initialized dim_users table with historical changes")
            
        else:
            # For subsequent loads, apply SCD Type 2 logic
            # 1. Find users with level changes
            spark.sql("""
                CREATE OR REPLACE TEMPORARY VIEW level_changes AS
                SELECT 
                    b.userId, 
                    b.firstName, 
                    b.lastName, 
                    b.gender, 
                    b.level AS new_level, 
                    b.registration_time,
                    b.timestamp,
                    d.level AS old_level,
                    d.userKey,
                    d.row_activation_date
                FROM users_batch b
                JOIN music_streaming.silver.dim_users d
                ON b.userId = d.userId AND d.current_row = 1
                WHERE b.level <> d.level
            """)
            
            # 2. Expire the current records for changed users
            spark.sql("""
                MERGE INTO music_streaming.silver.dim_users d
                USING level_changes c
                ON d.userKey = c.userKey AND d.current_row = 1
                WHEN MATCHED THEN
                    UPDATE SET 
                        row_expiration_date = CAST(date_trunc('day', c.timestamp) AS DATE),
                        current_row = 0
            """)
            
            # 3. Insert new records for users with level changes
            spark.sql("""
                INSERT INTO music_streaming.silver.dim_users
                SELECT 
                    md5(concat_ws('|', userId, new_level, timestamp)) AS userKey,
                    userId,
                    firstName,
                    lastName,
                    gender,
                    new_level AS level,
                    registration_time,
                    CAST(date_trunc('day', timestamp) AS DATE) AS row_activation_date,
                    CAST('9999-12-31' AS DATE) AS row_expiration_date,
                    1 AS current_row
                FROM level_changes
            """)
            
            # 4. Insert completely new users (not in the dimension yet)
            spark.sql("""
                INSERT INTO music_streaming.silver.dim_users
                SELECT 
                    md5(concat_ws('|', b.userId, b.level, b.timestamp)) AS userKey,
                    b.userId,
                    b.firstName,
                    b.lastName,
                    b.gender,
                    b.level,
                    b.registration_time,
                    CAST(date_trunc('day', b.timestamp) AS DATE) AS row_activation_date,
                    CAST('9999-12-31' AS DATE) AS row_expiration_date,
                    1 AS current_row
                FROM users_batch b
                LEFT JOIN music_streaming.silver.dim_users d
                ON b.userId = d.userId
                WHERE d.userId IS NULL
            """)
    
    # Write dim_users to Silver layer with streaming, using foreachBatch to handle SCD2
    users_query = (users_stream.writeStream
        .foreachBatch(process_users_scd2)
        .option("checkpointLocation", "/tmp/delta/_checkpoints/silver_dim_users")
        .trigger(availableNow=True)
        .start())
    
    # Dimension: Songs - streaming update
    songs_stream = (spark.readStream.table("music_streaming.bronze.listen_events")
        .select("song", "artist", "duration", "timestamp")
        .filter(F.col("song").isNotNull() & F.col("artist").isNotNull())
        .withColumnRenamed("song", "title")
        .withColumnRenamed("artist", "artistName")
        .withColumn("duration", F.col("duration").cast("double"))
        .withColumn("songKey", md5_hash("title", "artistName"))
        .dropDuplicates(["title", "artistName"])  # Simple deduplication for streaming
    )
    
    # Write dim_songs to Silver layer with streaming
    songs_query = (songs_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", "/tmp/delta/_checkpoints/silver_dim_songs")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable("music_streaming.silver.dim_songs"))
    
    # Dimension: Artists - streaming update
    artists_stream = (spark.readStream.table("music_streaming.bronze.listen_events")
        .select("artist", "timestamp")
        .filter(F.col("artist").isNotNull())
        .withColumnRenamed("artist", "name")
        .withColumn("artistKey", md5_hash("name"))
        .dropDuplicates(["name"])  # Simple deduplication for streaming
    )
    
    # Write dim_artists to Silver layer with streaming
    artists_query = (artists_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", "/tmp/delta/_checkpoints/silver_dim_artists")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable("music_streaming.silver.dim_artists"))
    
    # Dimension: Location - streaming update
    location_stream = (spark.readStream.table("music_streaming.bronze.listen_events")
        .select("city", "state", "zip", "latitude", "longitude", "timestamp")
        .filter(F.col("city").isNotNull() & F.col("state").isNotNull())
        .withColumnRenamed("state", "stateCode")
        .withColumn("locationKey", md5_hash("city", "stateCode", F.coalesce(F.col("zip"), F.lit("unknown"))))
        .dropDuplicates(["city", "stateCode", "zip", "latitude", "longitude"])  # Simple deduplication for streaming
    )
    
    # Write dim_location to Silver layer with streaming
    location_query = (location_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", "/tmp/delta/_checkpoints/silver_dim_location")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable("music_streaming.silver.dim_location"))
    
    # Dimension: DateTime - streaming update
    datetime_stream = (spark.readStream.table("music_streaming.bronze.listen_events")
        .select(F.date_trunc("hour", F.col("timestamp")).alias("date_hour"))
        .withColumn("hour", F.hour("date_hour"))
        .withColumn("dayOfMonth", F.dayofmonth("date_hour"))
        .withColumn("month", F.month("date_hour"))
        .withColumn("year", F.year("date_hour"))
        .withColumn("dayOfWeek", F.dayofweek("date_hour"))
        .withColumn("dateKey", F.md5(F.col("date_hour").cast("string")))
        .withColumnRenamed("date_hour", "date")
        .dropDuplicates(["date"])  # Simple deduplication for streaming
    )
    
    # Write dim_datetime to Silver layer with streaming
    datetime_query = (datetime_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", "/tmp/delta/_checkpoints/silver_dim_datetime")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable("music_streaming.silver.dim_datetime"))
    
    # Return the streaming queries so they can be managed
    return {
        "users_query": users_query,
        "songs_query": songs_query,
        "artists_query": artists_query,
        "location_query": location_query,
        "datetime_query": datetime_query
    }

# Execute the function to start streaming
silver_streaming_queries = create_silver_layer_dimensions_streaming()
```

### Step 4: Create Gold Layer Tables with Streaming

Create the fact table and denormalized view in the Gold layer with streaming:

```python
# Function to create Gold layer tables with streaming
def create_gold_layer_streaming():
    # We'll need to join with pre-loaded dimension tables
    # For streaming joins with static tables, we can load the dimensions first
    
    # Create the fact_streams table (fact table) with streaming
    listen_events_stream = (spark.readStream.table("music_streaming.bronze.listen_events")
        .select(
            "userId",
            "artist",
            "song",
            "timestamp",
            "city",
            "state",
            "latitude",
            "longitude"
        )
        .filter(
            F.col("userId").isNotNull() & 
            F.col("artist").isNotNull() & 
            F.col("song").isNotNull()
        )
        .withColumn("date_part", F.to_date("timestamp"))
    )
    
    # Define a foreachBatch function to handle the joining with dimension tables
    def process_fact_batch(batch_df, batch_id):
        # If the batch is empty, skip processing
        if batch_df.isEmpty():
            return
            
        # Load dimension tables from Silver layer
        dim_users = spark.table("music_streaming.silver.dim_users")
        dim_artists = spark.table("music_streaming.silver.dim_artists") 
        dim_songs = spark.table("music_streaming.silver.dim_songs")
        dim_location = spark.table("music_streaming.silver.dim_location")
        dim_datetime = spark.table("music_streaming.silver.dim_datetime")
        
        # Join with dimension tables
        fact_batch = (batch_df
            .join(dim_users, batch_df["userId"] == dim_users["userId"], "left")
            .join(dim_artists, batch_df["artist"] == dim_artists["name"], "left")
            .join(
                dim_songs, 
                (batch_df["song"] == dim_songs["title"]) & 
                (batch_df["artist"] == dim_songs["artistName"]), 
                "left"
            )
            .join(
                dim_location, 
                (batch_df["city"] == dim_location["city"]) & 
                (batch_df["state"] == dim_location["stateCode"]) & 
                (batch_df["latitude"] == dim_location["latitude"]) & 
                (batch_df["longitude"] == dim_location["longitude"]), 
                "left"
            )
            .join(
                dim_datetime, 
                F.date_trunc("hour", batch_df["timestamp"]) == dim_datetime["date"], 
                "left"
            )
            .select(
                dim_users["userKey"].alias("userKey"),
                dim_artists["artistKey"].alias("artistKey"),
                dim_songs["songKey"].alias("songKey"),
                dim_datetime["dateKey"].alias("dateKey"),
                dim_location["locationKey"].alias("locationKey"),
                batch_df["timestamp"].alias("ts"),
                batch_df["date_part"]
            )
        )
        
        # Write fact_batch to Gold layer
        (fact_batch.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .partitionBy("date_part")
            .saveAsTable("music_streaming.gold.fact_streams"))
            
        # Now, create/update the wide_streams view
        update_wide_streams()

    # Function to update the wide_streams table
    def update_wide_streams():
        # Get the fact table
        fact_streams = spark.table("music_streaming.gold.fact_streams")
        
        # Get dimension tables
        dim_users = spark.table("music_streaming.silver.dim_users")
        dim_artists = spark.table("music_streaming.silver.dim_artists") 
        dim_songs = spark.table("music_streaming.silver.dim_songs")
        dim_location = spark.table("music_streaming.silver.dim_location")
        dim_datetime = spark.table("music_streaming.silver.dim_datetime")
        
        # Create wide_streams (denormalized view for analytics)
        wide_streams = (fact_streams.alias("f")
            .join(dim_users.alias("u"), F.col("f.userKey") == F.col("u.userKey"), "inner")
            .join(dim_songs.alias("s"), F.col("f.songKey") == F.col("s.songKey"), "inner")
            .join(dim_location.alias("l"), F.col("f.locationKey") == F.col("l.locationKey"), "inner")
            .join(dim_datetime.alias("d"), F.col("f.dateKey") == F.col("d.dateKey"), "inner")
            .join(dim_artists.alias("a"), F.col("f.artistKey") == F.col("a.artistKey"), "inner")
            .select(
                F.col("f.userKey"),
                F.col("f.artistKey"),
                F.col("f.songKey"),
                F.col("f.dateKey"),
                F.col("f.locationKey"),
                F.col("f.ts").alias("timestamp"),
                F.col("u.firstName"),
                F.col("u.lastName"),
                F.col("u.gender"),
                F.col("u.level"),
                F.col("u.userId"),
                F.col("s.duration").alias("songDuration"),
                F.col("s.title").alias("songName"),
                F.col("l.city"),
                F.col("l.stateCode").alias("state"),
                F.col("l.latitude"),
                F.col("l.longitude"),
                F.col("d.date").alias("dateHour"),
                F.col("d.dayOfMonth"),
                F.col("d.dayOfWeek"),
                F.col("a.name").alias("artistName")
            )
        )
        
        # Write wide_streams to Gold layer - this is a full refresh based on current fact table
        (wide_streams.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable("music_streaming.gold.wide_streams"))
    
    # Start the streaming query with foreachBatch to handle the complex joins
    fact_query = (listen_events_stream.writeStream
        .foreachBatch(process_fact_batch)
        .option("checkpointLocation", "/tmp/delta/_checkpoints/gold_fact_streams")
        .trigger(availableNow=True)  # Process available data once and stop
        .start())
    
    # Return the streaming query so it can be managed
    return {
        "fact_query": fact_query
    }

# Execute the function to start streaming
gold_streaming_queries = create_gold_layer_streaming()
```

### Step 5: Monitoring Streaming Queries

```python
# Function to monitor all streaming queries
def monitor_streams(streaming_queries_dict):
    for name, query in streaming_queries_dict.items():
        print(f"Query Name: {name}")
        print(f"Status: {query.status}")
        print(f"Recent progress:")
        for progress in query.recentProgress:
            print(f"  - Batch: {progress.batchId}, records: {progress.numInputRows}, processing time: {progress.batchDuration} ms")
        print("-" * 50)

# Monitor all streaming queries
print("Bronze Layer Streaming Queries:")
monitor_streams(streaming_queries)

print("\nSilver Layer Streaming Queries:")
monitor_streams(silver_streaming_queries)

print("\nGold Layer Streaming Queries:")
monitor_streams(gold_streaming_queries)
```

### Step 6: Sample Analytics Queries with PySpark

```python
# The analytics queries remain the same, as they run on the final tables
# Top songs by play count
top_songs = (spark.table("music_streaming.gold.wide_streams")
    .groupBy("songName", "artistName")
    .agg(F.count("*").alias("play_count"))
    .orderBy(F.desc("play_count"))
    .limit(10)
)
display(top_songs)

# User activity by hour of day
hourly_activity = (spark.table("music_streaming.gold.wide_streams")
    .withColumn("hour_of_day", F.hour("dateHour"))
    .groupBy("hour_of_day")
    .agg(F.count("*").alias("song_plays"))
    .orderBy("hour_of_day")
)
display(hourly_activity)

# User demographics by level (free/paid)
user_demographics = (spark.table("music_streaming.gold.wide_streams")
    .groupBy("level", "gender")
    .agg(F.countDistinct("userId").alias("user_count"))
    .orderBy("level", "gender")
)
display(user_demographics)

# Geographic distribution of listening
geographic_distribution = (spark.table("music_streaming.gold.wide_streams")
    .groupBy("state")
    .agg(F.count("*").alias("listen_count"))
    .orderBy(F.desc("listen_count"))
)
display(geographic_distribution)
```

### Step 7: Gracefully Stopping All Streams

```python
# Function to stop all streaming queries
def stop_all_streams():
    # Stop Bronze layer streams
    for name, query in streaming_queries.items():
        print(f"Stopping {name}...")
        query.stop()
    
    # Stop Silver layer streams
    for name, query in silver_streaming_queries.items():
        print(f"Stopping {name}...")
        query.stop()
    
    # Stop Gold layer streams
    for name, query in gold_streaming_queries.items():
        print(f"Stopping {name}...")
        query.stop()
    
    print("All streaming queries stopped")

# To stop all streams when needed
# stop_all_streams()
```

## Best Practices for Streaming in Medallion Architecture

1. **Checkpointing**
   - Always use checkpoints for streaming jobs
   - Store checkpoints in a reliable location
   - Use different checkpoint locations for each query

2. **Handling Late Data**
   - Use watermarking for time-based operations
   - Consider window-based aggregations for late data
   - Implement proper error handling for data quality issues

3. **Performance Optimization**
   - Set appropriate trigger intervals based on data volume and latency requirements
   - Monitor streaming metrics and adjust resources accordingly
   - Partition output by date or other high-cardinality columns

4. **Error Handling**
   - Implement proper error handling strategies (e.g., dead-letter queues)
   - Use try-catch blocks in foreachBatch functions
   - Monitor for failures and implement retry mechanisms

5. **Scaling Considerations**
   - Tune executor and driver memory based on data volume
   - Consider using auto-scaling clusters for variable workloads
   - Balance parallelism with resource constraints

6. **Testing and Deployment**
   - Test with representative data volumes and patterns
   - Implement proper monitoring and alerting
   - Have a rollback strategy for failed deployments 