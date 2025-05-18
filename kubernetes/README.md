# Kubernetes Deployment

This directory contains Kubernetes manifests for deploying Kafka on Azure Kubernetes Service (AKS).

## Directory Structure

```
kubernetes/
└── kafka/                # Kafka cluster deployment
    ├── values.yaml       # Helm chart values for Kafka
    └── kafka-topics.md   # Documentation for Kafka topics
```

## Prerequisites

1. **Access to AKS Cluster**
   ```bash
   az aks get-credentials --resource-group musicstreamapp-final-dev-rg --name musicstreamapp-final-dev-aks
   ```

2. **Helm Installation**
   ```bash
   # Install Helm
   choco install kubernetes-helm
   ```

3. **Required Namespaces**
   ```bash
   kubectl create namespace kafka
   ```

## Deployment Instructions

### 1. Kafka Deployment

1. **Deploy Kafka using Helm**
   ```bash
   # Add the Bitnami Helm repository
   helm repo add bitnami https://charts.bitnami.com/bitnami
   helm repo update

   # Deploy Kafka using the values.yaml file (using version 24.0.5)
   cd kubernetes/kafka
   helm install kafka bitnami/kafka --version 24.0.5 -f values.yaml -n kafka
   ```

2. **Configure External Access - IMPORTANT**

   Before deploying Kafka, you need to update the `advertised.listeners` in the `values.yaml` file with the correct external IP address:

   ```yaml
   # Find this section in values.yaml
   broker:
     # ...other configurations...
     extraConfig: |
       # ...other configs...
       # Update the external IP address in the advertised.listeners
       advertised.listeners=INTERNAL://kafka-broker-0.kafka-broker-headless.kafka.svc.cluster.local:9093,EXTERNAL://YOUR_EXTERNAL_IP:9092
   ```

   To find your actual external IP:
   ```bash
   # After deploying Kafka
   kubectl get services -n kafka
   # Look for the EXTERNAL-IP of the kafka service
   ```

   If the external IP is different from what's configured in values.yaml:
   ```bash
   # Update the values.yaml file with the correct IP
   # Then upgrade the Kafka deployment
   helm upgrade --install kafka bitnami/kafka -n kafka -f values.yaml
   ```

3. **Create Kafka Topics**

   Once Kafka is properly deployed and the external IP is correctly configured, create the required topics:

   ```bash
   # Create the listen_events topic
   kubectl exec -it kafka-broker-0 -n kafka -- kafka-topics.sh --create --topic listen_events --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

   # Create the page_view_events topic
   kubectl exec -it kafka-broker-0 -n kafka -- kafka-topics.sh --create --topic page_view_events --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

   # Create the auth_events topic
   kubectl exec -it kafka-broker-0 -n kafka -- kafka-topics.sh --create --topic auth_events --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

   # Create the status_change_events topic
   kubectl exec -it kafka-broker-0 -n kafka -- kafka-topics.sh --create --topic status_change_events --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
   ```

   Verify that all topics were created correctly:
   ```bash
   kubectl exec -it kafka-broker-0 -n kafka -- kafka-topics.sh --list --bootstrap-server localhost:9092
   ```

   Expected output:
   ```
   auth_events
   listen_events
   page_view_events
   status_change_events
   ```

4. **Verify Kafka Service**
   ```bash
   # Check all services in the Kafka namespace
   kubectl get svc -n kafka
   
   # Check all pods in the Kafka namespace
   kubectl get pods -n kafka
   ```

## Testing

### 1. Verify Kafka Deployment

1. **Check Pods**
   ```bash
   kubectl get pods -n kafka
   ```

2. **Test Kafka Connection**
   ```bash
   # Get into a Kafka broker pod
   kubectl exec -it kafka-broker-0 -n kafka -- bash
   
   # List topics
   kafka-topics.sh --bootstrap-server localhost:9092 --list
   
   # Describe a topic to see partition assignments
   kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic listen_events
   ```

3. **Test External Kafka Connection**
   
   Use the event generator to verify external connectivity:
   
   ```bash
   # Deploy the event generator to ACI
   cd event_generator/deploy
   ./Deploy-To-ACI.ps1
   
   # Check the logs to verify successful connections
   az container logs --resource-group musicstreamapp-final-dev-rg --name event-generator
   ```

## Maintenance

### 1. Scaling

1. **Scale Kafka Brokers**
   ```bash
   kubectl scale statefulset kafka -n kafka --replicas=3
   ```

### 2. Updates

1. **Update Kafka Configuration**
   ```bash
   # Edit the values.yaml file with your changes
   # Then apply the changes using Helm
   helm upgrade kafka bitnami/kafka -n kafka -f values.yaml
   ```

## Troubleshooting

### 1. Common Issues

1. **Pod Not Starting**
   ```bash
   # Check pod logs
   kubectl logs -n kafka kafka-broker-0
   
   # Check pod events
   kubectl describe pod -n kafka kafka-broker-0
   ```

2. **Storage Issues**
   ```bash
   # Check persistent volumes
   kubectl get pv
   kubectl get pvc -n kafka
   ```

3. **Kafka Connection Issues**

   If you're having trouble connecting to Kafka:
   
   ```bash
   # Verify the external IP matches what's in your values.yaml
   kubectl get svc kafka -n kafka
   
   # Update values.yaml with the correct external IP in advertised.listeners
   # Then upgrade Kafka with:
   helm upgrade --install kafka bitnami/kafka -n kafka -f values.yaml
   ```

### 2. Network Issues

1. **Check Services**
   ```bash
   kubectl get svc -n kafka
   ```

2. **Check Network Policies**
   ```bash
   kubectl get networkpolicies -n kafka
   ```

## Monitoring

### 1. Resource Usage

```bash
# Check resource usage
kubectl top pods -n kafka

# Check node resources
kubectl top nodes
```

### 2. Logs

```bash
# Kafka logs
kubectl logs -f -n kafka kafka-broker-0
```

## References

- [Kafka Documentation](https://kafka.apache.org/documentation/)
- [Kubernetes Documentation](https://kubernetes.io/docs/home/)
- [Helm Documentation](https://helm.sh/docs/) 