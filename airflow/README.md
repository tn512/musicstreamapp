# Airflow Event Generator for Kafka

This directory contains Airflow DAGs and plugins for generating test events and sending them to Kafka topics.

## Structure

```
airflow/
├── dags/                      # Airflow DAG definitions
│   ├── kafka_event_generator_test.py       # Simple test DAG
│   └── user_activity_generator_dag.py      # Advanced user activity generator DAG
├── plugins/                   # Airflow plugins
│   └── event_generators/      # Event generator modules
│       ├── __init__.py
│       ├── base_generator.py              # Base event generator class
│       └── user_activity_generator.py     # User activity event generator
└── requirements.txt           # Python dependencies
```

## Requirements

The following Python packages are required:

```
apache-airflow>=2.7.0
kafka-python>=2.0.2
faker>=18.13.0
```

## Kafka Topics

The following Kafka topics are used by the DAGs:

- `quicktest`: Used by the simple test DAG (`kafka_event_generator_test.py`)
- `user-activity`: Used by the advanced user activity generator DAG (`user_activity_generator_dag.py`)

Both topics have been created with 2 partitions and a replication factor of 2.

## Usage

### Testing Kafka Connectivity

The `kafka_event_generator_test.py` DAG is a simple test that generates random events and sends them to the `quicktest` Kafka topic. This DAG is useful for verifying that Airflow can connect to Kafka and send messages successfully.

### Generating User Activity Events

The `user_activity_generator_dag.py` DAG uses the `UserActivityEventGenerator` class to generate realistic user activity events and send them to the `user-activity` Kafka topic. This DAG is scheduled to run every 15 minutes and generates 100 events per run.

## Event Generators

### BaseEventGenerator

The `BaseEventGenerator` class provides common functionality for connecting to Kafka and sending events. It handles:

- Connecting to Kafka
- Serializing events to JSON
- Sending events to Kafka topics
- Error handling and retries

### UserActivityEventGenerator

The `UserActivityEventGenerator` class extends `BaseEventGenerator` to generate realistic user activity events such as page views, clicks, purchases, etc. It uses the Faker library to generate realistic data.

## Integration with Existing Event Generator

If you already have an event generator in the `event_generator` folder, you can:

1. Use it directly in your DAGs
2. Extend the `BaseEventGenerator` class to create a wrapper around your existing generator
3. Create a new DAG that uses both your existing generator and the new generators

## Creating a New Kafka Topic

Before running the DAGs, make sure to create the required Kafka topics:

```bash
# The quicktest topic is already created
kubectl exec -it -n kafka kafka-broker-0 -- /opt/bitnami/kafka/bin/kafka-topics.sh --create --topic user-activity --partitions 2 --replication-factor 2 --bootstrap-server localhost:9092
```

## Monitoring

You can monitor the events being sent to Kafka by consuming from the topics:

```bash
# Monitor the quicktest topic
kubectl exec -it -n kafka kafka-broker-0 -- /opt/bitnami/kafka/bin/kafka-console-consumer.sh --topic quicktest --from-beginning --bootstrap-server localhost:9092

# Monitor the user-activity topic
kubectl exec -it -n kafka kafka-broker-0 -- /opt/bitnami/kafka/bin/kafka-console-consumer.sh --topic user-activity --from-beginning --bootstrap-server localhost:9092
``` 