# Music Streaming Analytics Project Documentation

This project implements a real-time data pipeline for a simulated music streaming service, similar to Spotify. The pipeline processes streaming events, stores them in a data lake, and creates analytics dashboards.

## Project Overview

The project consists of the following main components:

1. **Infrastructure (Terraform)**
   - Azure resources provisioning
   - Network configuration
   - Security settings

2. **Container Orchestration (Kubernetes)**
   - Kafka cluster deployment
   - Airflow deployment
   - Container management

3. **Event Generation**
   - Python-based event generator
   - Simulates user behavior
   - Produces streaming events

4. **Data Pipeline**
   - Real-time processing (every 2 minutes)
   - Batch processing (hourly)
   - Data quality checks

5. **Analytics**
   - Data transformation
   - Dashboard creation
   - Metrics calculation

## Project Structure

```
.
├── airflow/              # Airflow DAGs and configurations
├── event_generator/      # Event generation application
├── kubernetes/          # Kubernetes manifests
├── terraform/           # Infrastructure as Code
│   ├── bootstrap/       # Bootstrap configuration
│   ├── main/           # Main infrastructure
│   └── modules/        # Reusable Terraform modules
└── docs/               # Project documentation
    ├── airflow/        # Airflow documentation
    ├── event_generator/# Event generator documentation
    ├── kubernetes/     # Kubernetes documentation
    └── terraform/      # Terraform documentation
```

## Getting Started

1. [Infrastructure Setup](terraform/README.md)
2. [Kubernetes Deployment](kubernetes/README.md)
3. [Event Generator Setup](event_generator/README.md)
4. [Airflow Configuration](airflow/README.md)

## Prerequisites

- Azure subscription
- Azure CLI installed
- Terraform installed
- kubectl installed
- Python 3.8+
- Docker installed

## Architecture

The project follows a modern data architecture:

1. **Data Ingestion**
   - Event generator produces streaming events
   - Events are sent to Kafka topics

2. **Data Processing**
   - Real-time processing via Databricks
   - Batch processing via Airflow
   - Data quality checks with Great Expectations

3. **Data Storage**
   - Azure Data Lake Gen 2
   - Delta Lake format
   - Structured tables for analytics

4. **Data Visualization**
   - PowerBI dashboards
   - Real-time metrics
   - Historical analytics

## Monitoring and Maintenance

- [Monitoring Guide](monitoring/README.md)
- [Maintenance Procedures](maintenance/README.md)
- [Troubleshooting Guide](troubleshooting/README.md)

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct and the process for submitting pull requests.

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details. 