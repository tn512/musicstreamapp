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

### Current Azure Container Instances Deployment

The event generator is deployed to Azure Container Instances (ACI) using Docker and Azure Container Registry (ACR). The deployment process is automated using the `Deploy-To-ACI.ps1` PowerShell script.

#### Prerequisites

- Azure CLI installed and logged in
- Docker Desktop running
- Access to Azure Container Registry (`musicstreamappnewdevacr`)
- Access to Azure Resource Group (`musicstreamapp-new-dev-rg`)

#### Deployment Steps

1. Navigate to the deploy directory:
```powershell
cd event_generator/deploy
```

2. Run the Deploy-To-ACI.ps1 script to build the Docker image, push it to ACR, and deploy to ACI:
```powershell
.\Deploy-To-ACI.ps1
```

This script:
- Builds the Docker image from the Dockerfile
- Tags it with a timestamp for versioning
- Logs in to the Azure Container Registry
- Pushes the image to ACR
- Creates a container instance in Azure Container Instances
- Configures environment variables for Kafka connection

#### Verifying Deployment

Check the status of the container:
```powershell
az container show --resource-group musicstreamapp-new-dev-rg --name event-generator --query instanceView.state
```

View container logs:
```powershell
az container logs --resource-group musicstreamapp-new-dev-rg --name event-generator
```

#### Current Configuration

- ACR Name: `musicstreamappnewdevacr`
- Resource Group: `musicstreamapp-new-dev-rg`
- Location: `eastus`
- Container Name: `event-generator`
- Kafka Bootstrap Servers: `172.171.33.73:9092`

#### Troubleshooting Deployment

If you encounter issues with ACR credentials, the deployment script now includes fallback mechanisms:
- It will first try to retrieve credentials using standard methods
- If that fails, it will attempt to use an access token instead
- The script handles proper error reporting and recovery

## Data Sources

The event generator uses pre-downloaded data files:
- `data/songs_analysis.txt.gz`: Song metadata from the Million Song Dataset
- `data/user_agent.csv`: List of user agents for realistic browser simulation

## Troubleshooting

### 1. Common Issues

1. **Kafka Connection Issues**
   - Verify Kafka is running and accessible
   - Check the Kafka bootstrap server IP and port (currently set to `172.171.33.73:9092`)
   - Verify network connectivity from ACI to the Kafka bootstrap servers

2. **Event Generation Issues**
   - Check application logs: `az container logs --resource-group musicstreamapp-new-dev-rg --name event-generator`
   - Verify data files exist and are readable
   - Check resource usage (CPU/memory) in the Azure portal

3. **Docker Build Issues**
   - Ensure Docker Desktop is running
   - Check Docker settings for appropriate resource allocation
   - Verify connectivity to Azure Container Registry

### 2. Debug Mode

Run the generator in debug mode for detailed logging:
```bash
python generator.py --kafka-bootstrap-servers <kafka-bootstrap-servers> --debug
```

## Verifying Events in Kafka

The event generator regularly logs successful message deliveries, showing which topics, partitions, and offsets messages are being sent to. You can verify these by checking the container logs.

## References

- [Kafka Python Client Documentation](https://kafka-python.readthedocs.io/en/master/)
- [Million Song Dataset](http://millionsongdataset.com/)
- [Python Logging](https://docs.python.org/3/library/logging.html)
- [Azure Container Instances Documentation](https://docs.microsoft.com/en-us/azure/container-instances/)
- [Azure Container Registry Documentation](https://docs.microsoft.com/en-us/azure/container-registry/) 