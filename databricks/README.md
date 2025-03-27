# Databricks Data Processing Pipeline

This directory contains the Databricks notebooks that implement the data processing pipeline for the Music Streaming Analytics Platform. The pipeline follows a medallion architecture pattern to transform raw streaming data into actionable insights.

## Pipeline Overview

The pipeline processes music streaming events from Kafka and transforms them through multiple layers of refinement:

1. **Raw Layer**: Initial ingestion of streaming events from Kafka
2. **Bronze Layer**: Cleaned and validated data
3. **Silver Layer**: Business-level transformations
4. **Gold Layer**: Aggregated and business-ready data

## Notebook Structure

The pipeline is implemented through a series of notebooks that must be executed in sequence:

### Setup and Configuration
- `00_Setup.ipynb`: Sets up Unity Catalog and storage locations
- `01_Initialize_Setting.ipynb`: Initializes common settings and configurations

### Data Processing
- `02_Ingest_From_Kafka_To_Raw.ipynb`: Streams data from Kafka to Raw layer
- `03_Process_From_Raw_To_Bronze.ipynb`: Cleans and validates data
- `04.1_Stream_Process_From_Bronze_To_Silver.ipynb`: Real-time processing to Silver layer
- `04.2_Batch_Process_From_Bronze_To_Silver.ipynb`: Batch processing to Silver layer
- `05_Process_From_Silver_To_Gold.ipynb`: Creates final aggregated views
- `06_Test.ipynb`: Testing and validation

## Event Types

The pipeline processes four types of events, each with its own schema:

1. **Page View Events**
   - Tracks user page navigation
   - Includes fields: page, method, status

2. **Listen Events**
   - Records music playback
   - Includes fields: artist, song, duration

3. **Auth Events**
   - Monitors authentication attempts
   - Includes fields: success, method, status

4. **Status Change Events**
   - Tracks user status changes
   - Includes fields: prevLevel, method, status, statusChangeType

All events share common base fields:
- ts (timestamp)
- sessionId
- userId
- auth
- level
- itemInSession
- city
- zip
- state
- userAgent
- lon (longitude)
- lat (latitude)
- firstName
- lastName
- gender
- registration

## Infrastructure Requirements

- Azure Data Lake Storage (ADLS Gen2)
- Unity Catalog enabled Databricks workspace
- Kafka cluster (running on AKS)
- Appropriate permissions and storage credentials configured

## Usage Instructions

1. **Initial Setup**
   ```sql
   -- Run 00_Setup.ipynb to create necessary catalogs and schemas
   CREATE CATALOG IF NOT EXISTS music_streaming;
   USE CATALOG music_streaming;
   CREATE SCHEMA IF NOT EXISTS raw;
   CREATE SCHEMA IF NOT EXISTS bronze;
   CREATE SCHEMA IF NOT EXISTS silver;
   CREATE SCHEMA IF NOT EXISTS gold;
   ```

2. **Configure Storage**
   - Ensure storage credentials are properly configured
   - Verify external locations are created for checkpoints and raw data

3. **Run the Pipeline**
   - Execute notebooks in sequence from 00 to 06
   - Monitor the processing through Databricks UI
   - Check logs for any errors or issues

## Best Practices

1. **Checkpointing**
   - The pipeline uses checkpointing to ensure data consistency
   - Checkpoints are stored in ADLS Gen2
   - Do not delete checkpoint files unless necessary

2. **Error Handling**
   - The pipeline includes error handling for data loss scenarios
   - Failed records are logged for investigation
   - Use the test notebook to validate data quality

3. **Performance Optimization**
   - Streaming jobs are optimized for batch processing
   - Use appropriate cluster configurations
   - Monitor resource utilization

## Troubleshooting

1. **Common Issues**
   - Kafka connectivity problems
   - Storage permission issues
   - Schema validation failures

2. **Solutions**
   - Verify network connectivity to Kafka
   - Check storage credentials and permissions
   - Review schema definitions and data formats

## Maintenance

1. **Regular Tasks**
   - Monitor pipeline health
   - Check storage usage
   - Review error logs
   - Update schemas as needed

2. **Backup and Recovery**
   - Regular backups of checkpoint data
   - Recovery procedures for failed jobs
   - Data validation after recovery

## Contributing

When adding new features or modifications:
1. Create a new branch
2. Test changes thoroughly
3. Update documentation
4. Submit for review

## License

This project is licensed under the MIT License - see the LICENSE file for details. 