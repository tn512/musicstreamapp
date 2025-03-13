# Kubernetes Deployment

This directory contains Kubernetes manifests for deploying Kafka and Airflow on Azure Kubernetes Service (AKS).

## Directory Structure

```
kubernetes/
├── kafka/              # Kafka cluster deployment
│   ├── zookeeper/      # Zookeeper deployment
│   ├── kafka/          # Kafka brokers deployment
│   └── kafka-manager/  # Kafka Manager UI
└── airflow/           # Airflow deployment
    ├── webserver/     # Airflow webserver
    ├── scheduler/     # Airflow scheduler
    ├── workers/       # Airflow workers
    └── dags/          # Airflow DAGs
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

1. **Deploy Zookeeper**
   ```bash
   cd kafka/zookeeper
   kubectl apply -f .
   ```

2. **Deploy Kafka Brokers**
   ```bash
   cd ../kafka
   kubectl apply -f .
   ```

3. **Deploy Kafka Manager (Optional)**
   ```bash
   cd ../kafka-manager
   kubectl apply -f .
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
   # Get into a Kafka pod
   kubectl exec -it -n kafka kafka-0 -- /bin/bash
   
   # Create a test topic
   kafka-topics.sh --create --topic test --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1
   
   # List topics
   kafka-topics.sh --list --bootstrap-server localhost:9092
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