# Databricks Notebooks

This directory contains Databricks notebooks for the music streaming analytics platform.

## Directory Structure

- `notebooks/`: Contains exported Databricks notebooks
  - `streaming/`: Streaming data ingestion notebooks from Kafka to Delta
  - `batch/`: Batch processing notebooks for analytics
  - `utilities/`: Utility notebooks for common functions

## Working with These Notebooks

### Option 1: Manual Import/Export

1. **Export from Databricks**
   - In Databricks, open the notebook you want to export
   - Click File > Export > Source File
   - Save the file in the appropriate subdirectory of this project

2. **Import to Databricks**
   - In Databricks, click Workspace > Import
   - Select a notebook file from this directory
   - Choose the language and import location

### Option 2: Using Databricks CLI

1. **Setup Databricks CLI**
   ```bash
   pip install databricks-cli
   databricks configure --token
   ```

2. **Export from Databricks to Local**
   ```bash
   databricks workspace export /path/to/notebook databricks/notebooks/your-notebook.py
   ```

3. **Import from Local to Databricks**
   ```bash
   databricks workspace import databricks/notebooks/your-notebook.py /path/to/notebook
   ```

### Option 3: Using Databricks Repos (Recommended)

1. **In Databricks Workspace**
   - Click Repos
   - Click Add Repo
   - Connect to your Git repository
   - You can now work directly with notebooks in Git

2. **Advantages**
   - Direct Git integration
   - Branch support
   - Commit/push/pull from Databricks UI

## Notebook Naming Conventions

- Use descriptive names that indicate the purpose
- Include a version number if applicable
- Examples:
  - `kafka_to_delta_ingest_v1.py`
  - `hourly_song_analytics.py`
  - `user_demographics_report.py` 