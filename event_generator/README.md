# Music Streaming Event Generator

This Python application generates simulated music streaming events and sends them to Kafka topics. It simulates user behavior including:
- Song listening events
- Page view events
- Authentication events
- User status change events

## Directory Structure

- `generator.py`: Main event generator script
- `requirements.txt`: Python dependencies
- `data/`: Contains data files used by the generator
- `deploy/`: Deployment-related files
  - `Dockerfile`: For building the event generator container
  - `Build-And-Push.ps1`: PowerShell script for building and pushing Docker images
  - `Deploy-To-ACI.ps1`: PowerShell script for deploying to Azure Container Instances
  - `DEPLOYMENT.md`: Detailed deployment instructions

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Run the event generator:
```bash
python generator.py --kafka-bootstrap-servers <kafka-bootstrap-servers>
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
- `page_view_events`
- `auth_events`
- `status_change_events`

## Deployment

### 1. Build and Push Docker Image

Navigate to the deploy directory and run the Build-And-Push.ps1 script:
```powershell
cd deploy
.\Build-And-Push.ps1
```

### 2. Deploy to Azure Container Instances

Navigate to the deploy directory and run the Deploy-To-ACI.ps1 script:
```powershell
cd deploy
.\Deploy-To-ACI.ps1
```

For detailed deployment instructions, see `deploy/DEPLOYMENT.md`.

## Data Sources

The event generator uses pre-downloaded data files:
- `data/songs_analysis.txt.gz`: Song metadata from the Million Song Dataset
- `data/user_agent.csv`: List of user agents for realistic browser simulation

## Troubleshooting

### 1. Common Issues

1. **Kafka Connection Issues**
   - Verify Kafka is running: `kubectl get pods -n kafka`
   - Check Kafka logs: `kubectl logs -n kafka kafka-broker-0`
   - Verify network connectivity to the Kafka bootstrap servers

2. **Event Generation Issues**
   - Check application logs: `az container logs --resource-group musicstreamapp-dev-rg --name event-generator`
   - Verify data files exist and are readable
   - Check memory usage

### 2. Debug Mode

Run the generator in debug mode for detailed logging:
```bash
python generator.py --kafka-bootstrap-servers <kafka-bootstrap-servers> --debug
```

## Verifying Events in Kafka

To check if events are being sent to Kafka:
```bash
kubectl exec -it kafka-broker-0 -n kafka -- kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic listen_events --from-beginning --max-messages 5
```

## References

- [Kafka Python Client Documentation](https://kafka-python.readthedocs.io/en/master/)
- [Million Song Dataset](http://millionsongdataset.com/)
- [Python Logging](https://docs.python.org/3/library/logging.html) 