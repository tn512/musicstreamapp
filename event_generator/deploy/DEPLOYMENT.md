# Event Generator Deployment Guide

This guide explains how to deploy the event generator for the music streaming service data pipeline.

## Prerequisites

- Azure CLI installed and configured
- Docker installed locally
- Access to an Azure Container Registry (ACR)
- Access to an Azure Kubernetes Service (AKS) cluster
- Kafka running in the AKS cluster with a single broker

## Option 1: Deploy to AKS (Preferred)

This option deploys the event generator directly to your AKS cluster where Kafka is running.

### Step 1: Build and Push the Docker Image

1. Navigate to the event_generator directory:
   ```bash
   cd event_generator
   ```

2. Edit the `build_and_push.sh` script to set your ACR name:
   ```bash
   ACR_NAME="your_acr_name"  # Replace with your actual ACR name
   ```

3. Make the script executable and run it:
   ```bash
   chmod +x build_and_push.sh
   ./build_and_push.sh
   ```

### Step 2: Deploy to AKS

1. Navigate to the Kubernetes deployment directory:
   ```bash
   cd ../kubernetes/event-generator
   ```

2. Edit the `deploy_to_aks.sh` script to set your environment variables:
   ```bash
   ACR_NAME="your_acr_name"  # Replace with your actual ACR name
   AKS_CLUSTER_NAME="your_aks_cluster"  # Replace with your AKS cluster name
   AKS_RESOURCE_GROUP="your_resource_group"  # Replace with your resource group
   ```

3. Make the script executable and run it:
   ```bash
   chmod +x deploy_to_aks.sh
   ./deploy_to_aks.sh
   ```

4. Verify the deployment:
   ```bash
   kubectl get pods | grep event-generator
   ```

## Option 2: Deploy to Azure Container Instances (Fallback)

If AKS resources are constrained, you can deploy to Azure Container Instances (ACI) instead.

### Step 1: Build and Push the Docker Image

Follow the same steps as in Option 1 to build and push the Docker image to ACR.

### Step 2: Ensure Kafka is Accessible

For ACI to connect to Kafka in AKS, make sure your Kafka broker is exposed via a LoadBalancer service. With our single-broker setup, the main Kafka service provides this access:

```bash
# Check the external IP of the Kafka service
kubectl get svc -n kafka
```

You should see the external IP address for the main Kafka service (e.g., 4.246.237.185).

### Step 3: Deploy to ACI

1. Navigate to the event_generator deploy directory:
   ```bash
   cd event_generator/deploy
   ```

2. Run the Deploy-To-ACI.ps1 script:
   ```powershell
   ./Deploy-To-ACI.ps1
   ```

   Or manually edit and run the script to set your environment variables:
   ```powershell
   $ACR_NAME="your_acr_name"  # Replace with your actual ACR name
   $RESOURCE_GROUP="your_resource_group"  # Replace with your resource group
   $LOCATION="eastus"  # Replace with your preferred Azure region
   $KAFKA_BOOTSTRAP_SERVERS="4.246.237.185:9092"  # Replace with your Kafka external IP
   ```

4. Verify the deployment:
   ```bash
   az container show --resource-group your_resource_group --name event-generator --query instanceView.state
   ```

## Monitoring the Event Generator

### For AKS Deployment

```bash
# View logs
kubectl logs -f deployment/event-generator

# Check resource usage
kubectl top pod -l app=event-generator
```

### For ACI Deployment

```bash
# View logs
az container logs --resource-group your_resource_group --name event-generator

# Check container status
az container show --resource-group your_resource_group --name event-generator --query instanceView.state
```

## Troubleshooting

### Connection Issues to Kafka

1. Verify Kafka is running:
   ```bash
   kubectl get pods -n kafka
   ```

2. Check if your Kafka service is properly exposed:
   ```bash
   kubectl get svc -n kafka
   ```

3. Check event generator logs for connection errors:
   ```bash
   # For AKS deployment
   kubectl logs -f deployment/event-generator
   
   # For ACI deployment
   az container logs --resource-group your_resource_group --name event-generator
   ```

### Resource Constraints

If you encounter resource constraints in AKS, consider:

1. Scaling down other deployments temporarily
2. Reducing resource requests for the event generator
3. Switching to the ACI deployment option 