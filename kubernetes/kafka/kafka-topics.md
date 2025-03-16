# Kafka Topics for Music Streaming Service

This document outlines the Kafka topics used in our music streaming service data pipeline and the steps to create them.

## Topics Overview

Our music streaming service uses the following Kafka topics:

1. **listen_events** - Captures user listening activity (songs played)
2. **page_view_events** - Tracks user navigation through the application
3. **auth_events** - Records user authentication events (login/logout)
4. **status_change_events** - Monitors changes in user account status

## Topic Configuration

Each topic is configured with:
- 3 partitions
- Replication factor of 1 (for development; increase for production)

## Creating Topics

The topics can be created using the Kafka command-line tools within the Kubernetes cluster.

### Prerequisites

- Access to the Kubernetes cluster
- kubectl CLI tool installed and configured

### Steps to Create Topics

1. **Connect to a Kafka broker pod**:

   ```bash
   # List Kafka pods
   kubectl get pods -n kafka
   
   # Example output:
   # NAME               READY   STATUS    RESTARTS   AGE
   # kafka-broker-0     1/1     Running   0          18h
   # kafka-broker-1     1/1     Running   0          18h
   # kafka-zookeeper-0  1/1     Running   0          28h
   ```

2. **Create the listen_events topic**:

   ```bash
   kubectl exec -it kafka-broker-0 -n kafka -- kafka-topics.sh \
     --create \
     --topic listen_events \
     --bootstrap-server localhost:9092 \
     --partitions 3 \
     --replication-factor 1
   ```

3. **Create the page_view_events topic**:

   ```bash
   kubectl exec -it kafka-broker-0 -n kafka -- kafka-topics.sh \
     --create \
     --topic page_view_events \
     --bootstrap-server localhost:9092 \
     --partitions 3 \
     --replication-factor 1
   ```

4. **Create the auth_events topic**:

   ```bash
   kubectl exec -it kafka-broker-0 -n kafka -- kafka-topics.sh \
     --create \
     --topic auth_events \
     --bootstrap-server localhost:9092 \
     --partitions 3 \
     --replication-factor 1
   ```

5. **Create the status_change_events topic**:

   ```bash
   kubectl exec -it kafka-broker-0 -n kafka -- kafka-topics.sh \
     --create \
     --topic status_change_events \
     --bootstrap-server localhost:9092 \
     --partitions 3 \
     --replication-factor 1
   ```

6. **Verify the topics were created**:

   ```bash
   kubectl exec -it kafka-broker-0 -n kafka -- kafka-topics.sh \
     --list \
     --bootstrap-server localhost:9092
   ```

   Expected output:
   ```
   __consumer_offsets
   auth_events
   listen_events
   page_view_events
   status_change_events
   ```

## Topic Usage in Event Generator

The event generator application sends different types of events to their respective topics:

- User listening activity → `listen_events`
- Page navigation events → `page_view_events`
- Login/logout events → `auth_events`
- Account status changes → `status_change_events`

## Deleting Topics (if needed)

To delete a topic:

```bash
kubectl exec -it kafka-broker-0 -n kafka -- kafka-topics.sh \
  --delete \
  --topic TOPIC_NAME \
  --bootstrap-server localhost:9092
```

Replace `TOPIC_NAME` with the name of the topic you want to delete.

## Troubleshooting

If you encounter issues with topic visibility in the list command but can still describe the topic, try using a different broker or using bash to execute the command:

```bash
kubectl exec -it kafka-broker-0 -n kafka -- /bin/bash -c "kafka-topics.sh --list --bootstrap-server localhost:9092"
```

This alternative approach may show all topics when the standard list command doesn't. 