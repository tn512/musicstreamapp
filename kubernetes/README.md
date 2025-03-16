# Kubernetes Deployment

This directory contains Kubernetes manifests for deploying Kafka and Airflow on Azure Kubernetes Service (AKS).

## Directory Structure

```
kubernetes/
├── kafka/                # Kafka cluster deployment
│   ├── values.yaml       # Helm chart values for Kafka
│   ├── kafka-topics.md   # Documentation for Kafka topics
│   └── kafka-external-service.yaml  # External service for broker 1
└── airflow/              # Airflow deployment
    ├── webserver/        # Airflow webserver
    ├── scheduler/        # Airflow scheduler
    ├── workers/          # Airflow workers
    └── dags/             # Airflow DAGs
```

## Prerequisites

1. **Access to AKS Cluster**
   ```bash
   az aks get-credentials --resource-group musicstreamapp-dev-rg --name musicstreamapp-dev-aks
   ```

2. **Helm Installation**
   ```bash
   # Install Helm
   choco install kubernetes-helm
   ```

3. **Required Namespaces**
   ```bash
   kubectl create namespace kafka
   kubectl create namespace airflow
   ```

## Deployment Instructions

### 1. Kafka Deployment

1. **Deploy Kafka using Helm**
   ```bash
   # Add the Bitnami Helm repository
   helm repo add bitnami https://charts.bitnami.com/bitnami
   helm repo update

   # Deploy Kafka using the values.yaml file
   cd kubernetes/kafka
   helm install kafka bitnami/kafka -f values.yaml -n kafka
   ```

2. **Deploy Kafka External Service for Broker 1**
   
   To expose Kafka broker 1 to external clients (like the event generator running in ACI):
   
   ```bash
   # Apply the external service configuration
   kubectl apply -f kafka/kafka-external-service.yaml
   ```
   
   This will:
   - Create a LoadBalancer service specifically targeting Kafka broker 1
   - Assign an external IP that can be used to connect to broker 1 from outside the cluster

3. **Verify the External Service**
   ```bash
   # Check the external service and note the EXTERNAL-IP
   kubectl get svc kafka-external -n kafka
   ```

### 2. Airflow Deployment

1. **Deploy Airflow Components**
   ```bash
   cd airflow
   kubectl apply -f webserver/
   kubectl apply -f scheduler/
   kubectl apply -f workers/
   ```

2. **Deploy DAGs**
   ```bash
   kubectl apply -f dags/
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
   
   # Create a test topic
   kafka-topics.sh --bootstrap-server localhost:9092 --create --topic test --replication-factor 2 --partitions 3
   
   # List topics
   kafka-topics.sh --bootstrap-server localhost:9092 --list
   
   # Describe a topic to see partition assignments
   kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic test
   ```

3. **Test External Kafka Connection**
   
   Use the event generator to verify external connectivity:
   
   ```bash
   # Deploy the event generator to ACI
   cd event_generator/deploy
   ./Deploy-To-ACI.ps1
   
   # Check the logs to verify successful connections
   az container logs --resource-group musicstreamapp-dev-rg --name event-generator
   ```

### 2. Verify Airflow Deployment

1. **Check Pods**
   ```bash
   kubectl get pods -n airflow
   ```

2. **Access Airflow UI**
   ```bash
   # Port forward Airflow webserver
   kubectl port-forward -n airflow svc/airflow-webserver 8080:8080
   ```
   Then open http://localhost:8080 in your browser

## Maintenance

### 1. Scaling

1. **Scale Kafka Brokers**
   ```bash
   kubectl scale statefulset kafka -n kafka --replicas=3
   ```

2. **Scale Airflow Workers**
   ```bash
   kubectl scale deployment airflow-worker -n airflow --replicas=3
   ```

### 2. Updates

1. **Update Kafka Configuration**
   ```bash
   kubectl apply -f kafka/kafka-configmap.yaml
   kubectl rollout restart statefulset kafka -n kafka
   ```

2. **Update Airflow Configuration**
   ```bash
   kubectl apply -f airflow/airflow-configmap.yaml
   kubectl rollout restart deployment airflow-webserver -n airflow
   kubectl rollout restart deployment airflow-scheduler -n airflow
   kubectl rollout restart deployment airflow-worker -n airflow
   ```

## Troubleshooting

### 1. Common Issues

1. **Pod Not Starting**
   ```bash
   # Check pod logs
   kubectl logs -n kafka kafka-0
   kubectl logs -n airflow airflow-webserver-0
   
   # Check pod events
   kubectl describe pod -n kafka kafka-0
   kubectl describe pod -n airflow airflow-webserver-0
   ```

2. **Storage Issues**
   ```bash
   # Check persistent volumes
   kubectl get pv
   kubectl get pvc -n kafka
   kubectl get pvc -n airflow
   ```

### 2. Network Issues

1. **Check Services**
   ```bash
   kubectl get svc -n kafka
   kubectl get svc -n airflow
   ```

2. **Check Network Policies**
   ```bash
   kubectl get networkpolicies -n kafka
   kubectl get networkpolicies -n airflow
   ```

3. **Kafka External Service Issues**

   If the external IP is not assigned:
   
   ```bash
   # Check the service status
   kubectl describe service kafka-external -n kafka
   
   # Verify your cloud provider supports LoadBalancer services
   ```
   
   To delete and recreate the external service:
   
   ```bash
   kubectl delete service kafka-external -n kafka
   # Then run the create script again
   ```

## Monitoring

### 1. Resource Usage

```bash
# Check resource usage
kubectl top pods -n kafka
kubectl top pods -n airflow

# Check node resources
kubectl top nodes
```

### 2. Logs

```bash
# Kafka logs
kubectl logs -f -n kafka kafka-0

# Airflow logs
kubectl logs -f -n airflow airflow-webserver-0
```

## References

- [Kafka Documentation](https://kafka.apache.org/documentation/)
- [Airflow Documentation](https://airflow.apache.org/docs/)
- [Kubernetes Documentation](https://kubernetes.io/docs/home/)
- [Helm Documentation](https://helm.sh/docs/)

# Kubernetes Configuration

This directory contains Kubernetes configuration files and scripts for the music streaming application.

## Kafka External Service

The Kafka external service exposes Kafka broker 1 to external clients, allowing them to connect to Kafka from outside the Kubernetes cluster.

### Understanding the External Service

The `kafka-external-service.yaml` file creates a LoadBalancer service that specifically targets Kafka broker 1:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: kafka-external
  namespace: kafka
  labels:
    app.kubernetes.io/name: kafka
    app.kubernetes.io/component: broker
    broker-id: "1"
spec:
  type: LoadBalancer
  ports:
  - port: 9092
    targetPort: 9092
    protocol: TCP
  selector:
    app.kubernetes.io/name: kafka
    app.kubernetes.io/component: broker
    statefulset.kubernetes.io/pod-name: kafka-broker-1
```

This service specifically targets the pod named `kafka-broker-1` using the `statefulset.kubernetes.io/pod-name` selector.

### Connecting to Kafka from External Clients

When connecting from external clients like the event generator:

1. Use the primary bootstrap server (exposed by the main Kafka service) for initial connections:
   ```
   # Get the external IP of the main Kafka service
   kubectl get svc kafka -n kafka
   ```

2. For connections to specific brokers, use the appropriate external IP:
   - Broker 0: Use the main Kafka service external IP
   - Broker 1: Use the kafka-external service external IP
   ```
   # Get the external IP for broker 1
   kubectl get svc kafka-external -n kafka
   ```

3. In the event generator, we use a custom DNS resolver to map broker hostnames to their respective external IPs.

### Troubleshooting

If you encounter connection issues:

1. Verify both services have external IPs assigned:
   ```bash
   kubectl get svc -n kafka
   ```

2. Check if the event generator logs show any connection errors:
   ```bash
   az container logs --resource-group musicstreamapp-dev-rg --name event-generator
   ```

3. Verify the topic partitions and their leader assignments:
   ```bash
   kubectl exec -it kafka-broker-0 -n kafka -- kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic page_view_events
   ```

### Cleanup

To delete the external service:

```bash
kubectl delete -f kafka/kafka-external-service.yaml
``` 