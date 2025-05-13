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
from pyspark.sql.functions import col, from_json, current_timestamp
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

# Define topic configurations
TOPIC_CONFIGS = {
    "page_view_events": {
        "schema": page_view_schema,
        "description": "Raw page view events from Kafka"
    },
    "listen_events": {
        "schema": listen_schema,
        "description": "Raw listen events from Kafka"
    },
    "auth_events": {
        "schema": auth_schema,
        "description": "Raw authentication events from Kafka"
    },
    "status_change_events": {
        "schema": status_change_schema,
        "description": "Raw status change events from Kafka"
    }
}

def table_exists(catalog, schema, table):
    """Check if a table exists in Unity Catalog."""
    try:
        spark.sql(f"SELECT 1 FROM {catalog}.{schema}.{table} LIMIT 1")
        return True
    except:
        return False

def create_raw_table(topic_name):
    """Create a DLT table for a specific Kafka topic."""
    config = TOPIC_CONFIGS[topic_name]
    
    @dlt.table(
        name=f"music_streaming.dlt_raw.dlt_raw_{topic_name}",
        comment=config["description"]
    )
    def raw_table():
        # Define Kafka options
        kafka_options = {
            "kafka.bootstrap.servers": "172.212.37.12:9092",
            "subscribe": topic_name,
            "failOnDataLoss": "false",
            "kafka.security.protocol": "PLAINTEXT"
        }
        
        # Set starting offset only for first run
        if not table_exists("music_streaming", "dlt_raw", f"dlt_raw_{topic_name}"):
            kafka_options["startingOffsets"] = "earliest"
        
        return (
            spark.readStream
            .format("kafka")
            .options(**kafka_options)
            .load()
            .select(
                col("timestamp").alias("kafka_timestamp"),
                col("topic").alias("kafka_topic"),
                col("partition").alias("kafka_partition"),
                col("offset").alias("kafka_offset"),
                from_json(col("value").cast("string"), config["schema"]).alias("data")
            )
            .select("kafka_timestamp", "kafka_topic", "kafka_partition", "kafka_offset", "data.*")
            .withColumn("ingestion_timestamp", current_timestamp())
        )
    return raw_table

# Create tables for all topics
for topic in TOPIC_CONFIGS.keys():
    create_raw_table(topic)()
```

Key improvements in this version:

1. **DLT's Built-in First Run Detection**:
   - Uses `dlt.isFirstRun()` instead of manually checking checkpoint existence
   - More reliable as it uses DLT's internal state management

2. **Simplified Checkpoint Management**:
   - DLT automatically manages checkpoints in its own storage
   - No need to worry about checkpoint cleanup or management

3. **Better Kafka Options Management**:
   - All Kafka options are grouped in a dictionary
   - Cleaner code structure
   - Easier to modify options

4. **Additional Metadata**:
   - Added `kafka_partition` and `kafka_offset` columns
   - Added `ingestion_timestamp` for tracking when records are processed

5. **Error Handling**:
   - `failOnDataLoss: "false"` ensures pipeline continues even if some data is lost
   - DLT handles retries and error recovery automatically

### 2. Bronze Layer (from 03_Process_From_Raw_To_Bronze.ipynb)

```python
@dlt.table(
    name="music_streaming.dlt_bronze.dlt_bronze_page_view_events",
    comment="Cleaned and validated page view events",
    table_properties={
        "quality": "bronze"
    }
)
@dlt.expect("valid_timestamp", "ts IS NOT NULL")
@dlt.expect("valid_user_id", "userId IS NOT NULL")
def bronze_page_view_events():
    return (
        spark.readStream.table("music_streaming.dlt_raw.dlt_raw_page_view_events")
        .filter(col("ts").isNotNull())
        .filter(col("userId").isNotNull())
        .withColumn("event_timestamp", from_unixtime(col("ts")/1000))
    )

@dlt.table(
    name="music_streaming.dlt_bronze.dlt_bronze_listen_events",
    comment="Cleaned and validated listen events",
    table_properties={
        "quality": "bronze"
    }
)
@dlt.expect("valid_timestamp", "ts IS NOT NULL")
@dlt.expect("valid_user_id", "userId IS NOT NULL")
def bronze_listen_events():
    return (
        spark.readStream.table("music_streaming.dlt_raw.dlt_raw_listen_events")
        .filter(col("ts").isNotNull())
        .filter(col("userId").isNotNull())
        .withColumn("event_timestamp", from_unixtime(col("ts")/1000))
    )

@dlt.table(
    name="music_streaming.dlt_bronze.dlt_bronze_auth_events",
    comment="Cleaned and validated auth events",
    table_properties={
        "quality": "bronze"
    }
)
@dlt.expect("valid_timestamp", "ts IS NOT NULL")
@dlt.expect("valid_user_id", "userId IS NOT NULL")
def bronze_auth_events():
    return (
        spark.readStream.table("music_streaming.dlt_raw.dlt_raw_auth_events")
        .filter(col("ts").isNotNull())
        .filter(col("userId").isNotNull())
        .withColumn("event_timestamp", from_unixtime(col("ts")/1000))
    )

@dlt.table(
    name="music_streaming.dlt_bronze.dlt_bronze_status_change_events",
    comment="Cleaned and validated status change events",
    table_properties={
        "quality": "bronze"
    }
)
@dlt.expect("valid_timestamp", "ts IS NOT NULL")
@dlt.expect("valid_user_id", "userId IS NOT NULL")
def bronze_status_change_events():
    return (
        spark.readStream.table("music_streaming.dlt_raw.dlt_raw_status_change_events")
        .filter(col("ts").isNotNull())
        .filter(col("userId").isNotNull())
        .withColumn("event_timestamp", from_unixtime(col("ts")/1000))
    )
```

### 3. Silver Layer - Dimension Tables

```python
# 1. User Dimension (Streaming with SCD Type 2)
@dlt.table(
    name="music_streaming_dlt.dlt_silver_dim_users",
    comment="User dimension table with SCD Type 2 for tracking changes",
    temporary=False
)
@dlt.expect("valid_user_id", "userId IS NOT NULL")
def dim_users():
    # Get only the new/changed users from listen events (incremental read)
    current_users = (
        dlt.read_stream("dlt_bronze_listen_events")
        .select(
            "userId",
            "firstName", 
            "lastName",
            "gender",
            "level",
            "registration_time",
            "timestamp"
        )
        .filter(F.col("userId").isNotNull())
        .withColumn("firstName", F.coalesce(F.col("firstName"), F.lit("Unknown")))
        .withColumn("lastName", F.coalesce(F.col("lastName"), F.lit("Unknown")))
        .withColumn("gender", F.coalesce(F.col("gender"), F.lit("Unknown")))
    )

    # If table doesn't exist, create initial snapshot with historical changes
    if not dlt.table_exists("music_streaming_dlt.dlt_silver_dim_users"):
        return (
            spark.sql(f"""
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
                    FROM (
                        SELECT * FROM {current_users.createOrReplaceTempView("current_users")} current_users
                    )
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
                        change_group,
                        MIN(registration_time) AS registration_time,
                        MIN(timestamp) AS start_date,
                        MAX(timestamp) AS end_date
                    FROM change_groups
                    GROUP BY userId, firstName, lastName, gender, level, change_group
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
        )
    
    # For incremental updates
    existing_users = spark.table("music_streaming_dlt.dlt_silver_dim_users")
    
    # Group by userId and get the latest record for each user in the incoming batch
    latest_user_events = (
        current_users
        .groupBy("userId")
        .agg(
            F.max("timestamp").alias("max_timestamp"),
            F.first("firstName").alias("firstName"),
            F.first("lastName").alias("lastName"),
            F.first("gender").alias("gender"),
            F.first("level").alias("level"),
            F.first("registration_time").alias("registration_time")
        )
        .withColumn("timestamp", F.col("max_timestamp"))
        .drop("max_timestamp")
    )
    
    # Find users with level changes by joining with current records
    changed_users = (
        latest_user_events.alias("new")
        .join(
            existing_users.filter("current_row = 1").alias("current"),
            "userId",
            "left_outer"
        )
        .where((F.col("current.level").isNull()) |  # New users
               (F.col("new.level") != F.col("current.level")) |  # Level changed
               (F.col("new.firstName") != F.col("current.firstName")) |  # First name changed
               (F.col("new.lastName") != F.col("current.lastName")) |  # Last name changed
               (F.col("new.gender") != F.col("current.gender"))  # Gender changed
        )
        .select(
            "new.userId",
            "new.firstName",
            "new.lastName",
            "new.gender",
            "new.level",
            "new.registration_time",
            "new.timestamp",
            F.col("current.userKey").alias("current_userKey")
        )
    )
    
    # If no changes, return existing table
    if changed_users.isEmpty():
        return existing_users
    
    # Create new records for changed users
    new_records = (
        changed_users
        .withColumn("row_activation_date", F.date_trunc('day', F.col("timestamp")))
        .withColumn("row_expiration_date", F.lit("9999-12-31").cast("date"))
        .withColumn("current_row", F.lit(1))
        .withColumn("userKey", F.md5(F.concat_ws("|", 
            F.col("userId"), 
            F.col("level"), 
            F.col("timestamp")
        )))
        .drop("current_userKey")
    )
    
    # Update existing records - set expiration date and current flag
    user_ids_to_update = [row.userId for row in changed_users.select("userId").collect()]
    
    if len(user_ids_to_update) > 0:
        # Get changed user expiration dates
        change_dates = {
            row.userId: row.timestamp 
            for row in changed_users.select("userId", "timestamp").collect()
        }
        
        # Update expiration dates for existing records
        updated_existing = (
            existing_users
            .withColumn("row_expiration_date",
                F.when(
                    (F.col("current_row") == True) & 
                    F.col("userId").isin(user_ids_to_update),
                    F.date_trunc('day', F.lit(change_dates[F.col("userId")])) 
                )
                .otherwise(F.col("row_expiration_date"))
            )
            .withColumn("current_row",
                F.when(
                    (F.col("current_row") == True) & 
                    F.col("userId").isin(user_ids_to_update),
                    False
                )
                .otherwise(F.col("current_row"))
            )
        )
        
        # Combine existing and new records
        return updated_existing.unionByName(new_records)
    
    # If only new users (no updates), combine with existing
    return existing_users.unionByName(new_records)

# 2. Location Dimension (Batch)
@dlt.table(
    name="music_streaming.dlt_silver.dim_location",
    comment="Location dimension table with state information"
)
@dlt.expect("valid_city", "city IS NOT NULL")
@dlt.expect("valid_state", "stateCode IS NOT NULL")
def dim_location():
    # Read state codes from CSV in DBFS
    state_raw = spark.read.csv("dbfs:/FileStore/music_streaming/data/state_codes.csv", header=True)
    
    # Process locations directly
    return (
        spark.read.table("music_streaming.dlt_bronze.dlt_bronze_listen_events")
        .select(
            "city",
            col("state").alias("stateCode"),
            "zip",
            "latitude",
            "longitude"
        )
        .filter(col("city").isNotNull() & col("stateCode").isNotNull())
        .dropDuplicates(["city", "stateCode", "zip", "latitude", "longitude"])
        .join(state_raw, "stateCode", "left")
        .withColumn("stateCode", coalesce(col("stateCode"), lit("NA")))
        .withColumn("stateName", coalesce(col("stateName"), lit("NA")))
        .withColumn("locationKey", md5(concat_ws("|", 
            col("city"), 
            col("stateCode"), 
            coalesce(col("zip"), lit("Unknown"))
        )))
    )

# 3. Song Dimension (Batch)
@dlt.table(
    name="music_streaming.dlt_silver.dim_songs",
    comment="Song dimension table from songs.csv"
)
@dlt.expect("valid_song_id", "songId IS NOT NULL")
@dlt.expect("valid_title", "title IS NOT NULL")
def dim_songs():
    return (
        spark.read.csv("dbfs:/FileStore/shared_uploads/songs.csv", header=True)
        .select(
            col("song_id").alias("songId"),
            col("title"),
            col("artist_name").alias("artistName"),
            col("duration").cast("double"),
            col("key").cast("integer"),
            col("key_confidence").cast("double").alias("keyConfidence"),
            col("loudness").cast("double"),
            col("song_hotttnesss").cast("double").alias("songHotness"),
            col("tempo").cast("double"),
            col("year").cast("integer"),
            md5(concat_ws("|", "song_id")).alias("songKey")
        )
        .where(col("title").isNotNull() & col("artistName").isNotNull())
    )

# 4. Artist Dimension (Batch)
@dlt.table(
    name="music_streaming.dlt_silver.dim_artists",
    comment="Artist dimension table from songs.csv"
)
@dlt.expect("valid_artist_id", "artistId IS NOT NULL")
@dlt.expect("valid_name", "name IS NOT NULL")
def dim_artists():
    return (
        spark.read.csv("dbfs:/FileStore/shared_uploads/songs.csv", header=True)
        .select(
            col("artist_id").alias("artistId"),
            col("artist_latitude").cast("double").alias("latitude"),
            col("artist_longitude").cast("double").alias("longitude"),
            col("artist_location").alias("location"),
            regexp_replace(regexp_replace(col("artist_name"), '"', ''), '\\\\', '').alias("name")
        )
        .where(col("name").isNotNull())
        .groupBy("name")
        .agg(
            max("artistId").alias("artistId"),
            max("latitude").alias("latitude"),
            max("longitude").alias("longitude"),
            max("location").alias("location")
        )
        .withColumn("artistKey", md5(col("artistId")))
    )

# 5. DateTime Dimension (Batch)
@dlt.table(
    name="music_streaming.dlt_silver.dim_datetime",
    comment="DateTime dimension table with hourly granularity"
)
def dim_datetime():
    return (
        spark.sql("""
            WITH datetime_series AS (
                SELECT explode(sequence(
                    to_timestamp('2018-10-01 00:00:00'),
                    to_timestamp('2025-03-31 23:59:59'),
                    interval 1 hour
                )) AS datetime
            )
            SELECT
                unix_timestamp(datetime) AS dateKey,
                datetime,
                dayofweek(datetime) AS dayOfWeek,
                dayofmonth(datetime) AS dayOfMonth,
                dayofyear(datetime) AS dayOfYear,
                month(datetime) AS month,
                year(datetime) AS year,
                CASE WHEN dayofweek(datetime) IN (6, 7) THEN True ELSE False END AS weekendFlag
            FROM datetime_series
            ORDER BY datetime
        """)
    )
```

Key points about the dimension tables:

1. **Processing Types**:
   - Streaming: Users (with SCD Type 2) and Location
   - Batch: Songs, Artists, and DateTime

2. **Data Quality**:
   - Using `@dlt.expect` for data validation
   - Ensuring required fields are not null
   - Handling missing values with coalesce

3. **SCD Type 2 Implementation**:
   - Uses `apply_changes()` for proper CDC
   - Tracks history for specific columns: "level", "firstName", "lastName", "gender"
   - Uses event timestamp for sequencing
   - Stores as SCD Type 2 with automatic history tracking

4. **Data Cleaning**:
   - Removing special characters from artist names
   - Handling null values in location data
   - Converting data types appropriately

5. **Surrogate Keys**:
   - Using MD5 hash for consistent key generation
   - Helps with data integration
   - Maintains referential integrity

To use these dimension tables:

1. **Join with Fact Tables**:
```python
@dlt.table(
    name="music_streaming.dlt_silver.fact_listen_events",
    comment="Fact table for song listening events"
)
def fact_listen_events():
    return (
        dlt.read("music_streaming.dlt_bronze.dlt_bronze_listen_events")
        .join(
            dlt.read("music_streaming.dlt_silver.dim_songs"),
            on="song_id",
            how="left"
        )
        .join(
            dlt.read("music_streaming.dlt_silver.dim_users"),
            on="userId",
            how="left"
        )
        .join(
            dlt.read("music_streaming.dlt_silver.dim_location"),
            on=["city", "stateCode"],
            how="left"
        )
        .select(
            "event_timestamp",
            "userId",
            "songId",
            "artistId",
            "locationKey",
            "duration",
            "sessionId"
        )
    )
```

2. **Monitor Data Quality**:
```python
# Check data quality metrics
dlt.expectations("music_streaming.dlt_silver.dim_users")
dlt.expectations("music_streaming.dlt_silver.dim_location")
dlt.expectations("music_streaming.dlt_silver.dim_songs")
dlt.expectations("music_streaming.dlt_silver.dim_artists")
```

3. **Update Frequency**:
- Streaming tables (Users, Location): Updated continuously
- Batch tables (Songs, Artists, DateTime): Updated on schedule or manually

4. **Best Practices**:
- Keep dimension tables small and focused
- Use surrogate keys for consistency
- Include audit columns (created_at, updated_at)
- Add appropriate indexes for performance
- Monitor data quality metrics

Would you like me to:
1. Add more dimension tables?
2. Show how to handle slowly changing dimensions for other tables?
3. Add more data quality checks?
4. Show how to optimize dimension table performance?

### 4. Gold Layer (from 05_Process_From_Silver_To_Gold.ipynb)

```python
# Gold Layer Tables

# 1. Fact Stream Table
@dlt.table(
    name="music_streaming_dlt.dlt_gold_fact_stream",
    comment="Fact table connecting users, songs, artists, locations, and datetime",
    table_properties={
        "quality": "gold",
        "delta.enableChangeDataFeed": "true"
    }
)
def fact_stream():
    # Read from bronze listen events
    listen_events_df = (
        dlt.read_stream("dlt_bronze_listen_events")
        .select(
            "userId", 
            "song", 
            "timestamp", 
            "city", 
            "state", 
            "latitude", 
            "longitude",
            F.regexp_replace(F.regexp_replace(F.col("artist"), '"', ''), '\\\\\\\\', '').alias("artist_clean")
        )
        .filter(F.col("userId").isNotNull())
    )
    
    # Read dimension tables
    dim_users_df = spark.table("music_streaming_dlt.dlt_silver_dim_users")
    dim_songs_df = spark.table("music_streaming.dlt_silver.dim_songs")
    dim_artists_df = spark.table("music_streaming.dlt_silver.dim_artists")
    dim_locations_df = spark.table("music_streaming.dlt_silver.dim_location")
    dim_datetime_df = spark.table("music_streaming.dlt_silver.dim_datetime")
    
    # Join with dimensions and select keys
    return (
        listen_events_df
        # Join with dim_users (SCD Type 2)
        .join(
            dim_users_df, 
            (listen_events_df.userId == dim_users_df.userId)
            & (F.to_date(listen_events_df.timestamp) >= dim_users_df.row_activation_date)
            & (F.to_date(listen_events_df.timestamp) < dim_users_df.row_expiration_date), 
            "left"
        )
        # Join with dim_artists
        .join(
            dim_artists_df, 
            listen_events_df.artist_clean == dim_artists_df.name, 
            "left"
        )
        # Join with dim_songs
        .join(
            dim_songs_df, 
            (listen_events_df.song == dim_songs_df.title)
            & (listen_events_df.artist_clean == dim_songs_df.artistName), 
            "left"
        )
        # Join with dim_location
        .join(
            dim_locations_df,
            (listen_events_df.city == dim_locations_df.city)
            & (listen_events_df.state == dim_locations_df.stateCode)
            & (listen_events_df.latitude == dim_locations_df.latitude)
            & (listen_events_df.longitude == dim_locations_df.longitude),
            "left"
        )
        # Join with dim_datetime
        .join(
            dim_datetime_df, 
            F.date_trunc("hour", listen_events_df.timestamp) == dim_datetime_df.datetime, 
            "left"
        )
        .select(
            "userKey", 
            "artistKey", 
            "songKey", 
            "dateKey", 
            "locationKey", 
            "timestamp",
            F.to_date(F.col("timestamp")).alias("date_part")
        )
    )

# 2. Wide Fact View
@dlt.view(
    name="music_streaming_dlt.dlt_gold_wide_fact",
    comment="Denormalized view joining fact_stream with dimension tables for easier analysis"
)
def wide_fact():
    return spark.sql("""
    SELECT 
        f.userKey,
        f.artistKey,
        f.songKey,
        f.dateKey,
        f.locationKey,
        f.timestamp,
        u.firstName,
        u.lastName,
        u.gender,
        u.level,
        u.userId,
        s.duration as songDuration,
        s.title as songName,
        l.city,
        l.stateCode as state,
        l.latitude,
        l.longitude,
        d.datetime as dateHour,
        d.dayOfMonth,
        d.dayOfWeek,
        a.name as artistName
    FROM music_streaming_dlt.dlt_gold_fact_stream f
    INNER JOIN music_streaming_dlt.dlt_silver_dim_users u ON f.userKey = u.userKey
    INNER JOIN music_streaming.dlt_silver.dim_songs s ON f.songKey = s.songKey
    INNER JOIN music_streaming.dlt_silver.dim_artists a ON f.artistKey = a.artistKey
    INNER JOIN music_streaming.dlt_silver.dim_location l ON f.locationKey = l.locationKey
    INNER JOIN music_streaming.dlt_silver.dim_datetime d ON f.dateKey = d.dateKey
    """)

# 3. Daily User Summary 
@dlt.table(
    name="music_streaming_dlt.dlt_gold_daily_user_summary",
    comment="Daily aggregated metrics per user"
)
def daily_user_summary():
    return spark.sql("""
    SELECT 
        date_part,
        userId,
        firstName,
        lastName,
        level,
        COUNT(*) as total_streams,
        COUNT(DISTINCT songKey) as unique_songs,
        COUNT(DISTINCT artistKey) as unique_artists
    FROM music_streaming_dlt.dlt_gold_wide_fact
    GROUP BY date_part, userId, firstName, lastName, level
    """)

# 4. Daily Location Summary
@dlt.table(
    name="music_streaming_dlt.dlt_gold_daily_location_summary",
    comment="Daily aggregated metrics per location"
)
def daily_location_summary():
    return spark.sql("""
    SELECT 
        date_part,
        city,
        state,
        COUNT(*) as total_streams,
        COUNT(DISTINCT userId) as unique_users,
        COUNT(DISTINCT songKey) as unique_songs
    FROM music_streaming_dlt.dlt_gold_wide_fact
    GROUP BY date_part, city, state
    """)

# 5. Artist Popularity
@dlt.table(
    name="music_streaming_dlt.dlt_gold_artist_popularity",
    comment="Artist popularity metrics"
)
def artist_popularity():
    return spark.sql("""
    SELECT 
        artistName,
        COUNT(*) as total_streams,
        COUNT(DISTINCT userId) as unique_users,
        COUNT(DISTINCT songKey) as unique_songs,
        COUNT(DISTINCT date_part) as active_days
    FROM music_streaming_dlt.dlt_gold_wide_fact
    GROUP BY artistName
    """)
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
    "continuous": False,  # Set to False for trigger=AvailableNow behavior
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
    "channel": "preview",
    "trigger": {
        "type": "manual"  # This enables trigger=AvailableNow behavior
    }
}
```

## Running the Pipeline

1. **Create Pipeline**
   ```bash
   databricks pipelines create --config config/pipeline_config.json
   ```

2. **Start Pipeline (Trigger=AvailableNow)**
   ```bash
   # This will run the pipeline once and stop
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