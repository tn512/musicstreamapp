# Music Streaming Analytics Platform

This repository contains the infrastructure and application code for a music streaming analytics platform. The platform collects, processes, and analyzes music streaming events to provide insights into user behavior and music trends.

## Architecture

The platform consists of the following components:

- **Event Generator**: Simulates music streaming events and sends them to Kafka
- **Kafka**: Message broker for event streaming
- **Airflow**: Orchestrates data processing workflows
- **Azure Kubernetes Service (AKS)**: Hosts Kafka and Airflow
- **Azure Container Instances (ACI)**: Alternative deployment option for the event generator
- **Azure Container Registry (ACR)**: Stores Docker images

## Directory Structure

```
.
├── airflow/            # Airflow DAGs and plugins
├── docs/               # Documentation
├── event_generator/    # Event generator application
├── kubernetes/         # Kubernetes deployment manifests
└── terraform/          # Infrastructure as Code
```

## Getting Started

### Prerequisites

- Azure subscription
- Azure CLI
- Docker
- Kubernetes CLI (kubectl)
- Terraform

### Infrastructure Deployment

1. **Deploy Azure Resources**

   ```bash
   cd terraform/main
   terraform init
   terraform apply
   ```

2. **Connect to AKS**

   ```bash
   az aks get-credentials --resource-group musicstreamapp-dev-rg --name musicstreamapp-dev-aks
   ```

### Kafka Deployment

1. **Deploy Kafka on AKS**

   ```bash
   cd kubernetes
   kubectl create namespace kafka
   kubectl apply -f kafka/
   ```

2. **Create Kafka External Service**

   ```bash
   # For Linux/Mac
   cd kubernetes
   chmod +x create-kafka-external.sh
   ./create-kafka-external.sh
   
   # For Windows
   cd kubernetes
   .\Create-KafkaExternal.ps1
   ```

### Testing Kafka Connectivity

1. **Run Kafka Test**

   ```bash
   # For Linux/Mac
   cd event_generator
   chmod +x run_local_test.sh
   ./run_local_test.sh
   
   # For Windows
   cd event_generator
   .\Run-LocalTest.ps1
   ```

2. **Using Docker**

   ```bash
   # For Linux/Mac
   cd event_generator
   chmod +x run_kafka_test.sh
   ./run_kafka_test.sh
   
   # For Windows
   cd event_generator
   .\Run-KafkaTest.ps1
   ```

## Documentation

- [Event Generator](event_generator/README.md)
- [Kafka Testing](event_generator/KAFKA_TESTING.md)
- [Kubernetes Deployment](kubernetes/README.md)
- [Airflow](airflow/README.md)
- [Terraform](terraform/README.md)

## License

This project is licensed under the MIT License - see the LICENSE file for details.