# Music Streaming Event Generator

This Python application generates simulated music streaming events and sends them to Kafka topics. It simulates user behavior including:
- Song listening events
- Page view events
- Authentication events
- User status change events

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Run the event generator:
```bash
python generator.py
```

## Event Types

1. **Listen Events**
   - User listening to songs
   - Includes: user_id, song_id, timestamp, duration

2. **Page View Events**
   - Users browsing pages/UI
   - Includes: user_id, page_name, timestamp

3. **Auth Events**
   - User login/logout activities
   - Includes: user_id, auth_type (success/failure), timestamp

4. **Status Change Events**
   - User account status changes
   - Includes: user_id, status_type, timestamp

## Kafka Topics

The generator sends events to the following Kafka topics:
- `listen_events`
- `page_views`
- `auth_events`
- `status_events`

## Configuration

The Kafka bootstrap server is configured to connect to:
`kafka-broker-0.kafka-headless.kafka.svc.cluster.local:9092`

## Data Sources

The event generator uses pre-downloaded data files:
- `data/songs_analysis.txt.gz`: Song metadata from the Million Song Dataset
- `data/user_agents.txt`: List of user agents for realistic browser simulation

## Testing

### 1. Test Kafka Connection
```bash
python test_kafka.py
```

### 2. Verify Event Generation
```bash
# Check if events are being sent to Kafka
kubectl exec -it -n kafka kafka-0 -- /bin/bash
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic listen_events --from-beginning
```

### 3. Monitor Event Rate
```bash
# Check the number of events generated per minute
python generator.py --monitor
```

## Troubleshooting

### 1. Common Issues

1. **Kafka Connection Issues**
   - Verify Kafka is running: `kubectl get pods -n kafka`
   - Check Kafka logs: `kubectl logs -n kafka kafka-0`
   - Verify network connectivity: `ping kafka-broker-0.kafka-headless.kafka.svc.cluster.local`

2. **Event Generation Issues**
   - Check application logs
   - Verify data files exist and are readable
   - Check memory usage

3. **Data Quality Issues**
   - Validate event schema
   - Check for missing or invalid data
   - Monitor event rates

### 2. Debug Mode

Run the generator in debug mode for detailed logging:
```bash
python generator.py --debug
```

## Performance Tuning

1. **Event Rate Control**
   ```bash
   # Adjust events per second
   python generator.py --rate 100
   ```

2. **User Simulation**
   ```bash
   # Control number of simulated users
   python generator.py --users 1000
   ```

3. **Resource Usage**
   ```bash
   # Monitor CPU and memory usage
   python generator.py --monitor-resources
   ```

## Development

### 1. Adding New Event Types

1. Create a new event class in `generator.py`
2. Add event generation logic
3. Update the main loop to include the new event type

### 2. Modifying Event Schema

1. Update the event class definition
2. Modify the event generation logic
3. Update any consumers of the events

## References

- [Kafka Python Client Documentation](https://kafka-python.readthedocs.io/en/master/)
- [Million Song Dataset](http://millionsongdataset.com/)
- [Python Logging](https://docs.python.org/3/library/logging.html) 