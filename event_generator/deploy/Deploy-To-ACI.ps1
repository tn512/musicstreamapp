# Configuration
$ACR_NAME = "musicstreamappdevacr"
$RESOURCE_GROUP = "musicstreamapp-dev-rg"
$LOCATION = "eastus"
$CONTAINER_NAME = "event-generator"
$TIMESTAMP = Get-Date -Format "yyyyMMddHHmmss"
$IMAGE_TAG = "v$TIMESTAMP"
$IMAGE_NAME = "$ACR_NAME.azurecr.io/event-generator:$IMAGE_TAG"
$KAFKA_BOOTSTRAP_SERVERS = "4.246.237.185:9092"  # Primary Kafka LoadBalancer service IP and port

# Build and push the Docker image with the new tag
Write-Host "Building and pushing Docker image with tag $IMAGE_TAG..." -ForegroundColor Green
docker build -t $IMAGE_NAME -f Dockerfile ..
az acr login --name $ACR_NAME
docker push $IMAGE_NAME

# Get ACR credentials
Write-Host "Getting ACR credentials..." -ForegroundColor Green
$ACR_USERNAME = az acr credential show --name $ACR_NAME --query "username" -o tsv
$ACR_PASSWORD = az acr credential show --name $ACR_NAME --query "passwords[0].value" -o tsv

# Delete existing container instance if it exists
Write-Host "Checking if container instance exists..." -ForegroundColor Green
$containerExists = az container show --resource-group $RESOURCE_GROUP --name $CONTAINER_NAME 2>$null
if ($containerExists) {
    Write-Host "Deleting existing container instance..." -ForegroundColor Yellow
    az container delete --resource-group $RESOURCE_GROUP --name $CONTAINER_NAME --yes
    Write-Host "Waiting for container deletion to complete..." -ForegroundColor Yellow
    Start-Sleep -Seconds 10
}

# Create container instance
Write-Host "Creating container instance in ACI..." -ForegroundColor Green
az container create `
  --resource-group $RESOURCE_GROUP `
  --name $CONTAINER_NAME `
  --image $IMAGE_NAME `
  --registry-login-server "$ACR_NAME.azurecr.io" `
  --registry-username $ACR_USERNAME `
  --registry-password $ACR_PASSWORD `
  --environment-variables KAFKA_BOOTSTRAP_SERVERS=$KAFKA_BOOTSTRAP_SERVERS `
  --cpu 1 `
  --memory 2 `
  --restart-policy Always `
  --location $LOCATION `
  --os-type Linux

Write-Host "Container instance created successfully!" -ForegroundColor Green
Write-Host "To check logs, run: az container logs --resource-group $RESOURCE_GROUP --name $CONTAINER_NAME" -ForegroundColor Cyan
Write-Host "To check status, run: az container show --resource-group $RESOURCE_GROUP --name $CONTAINER_NAME --query instanceView.state" -ForegroundColor Cyan 