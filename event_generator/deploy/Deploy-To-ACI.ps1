# Configuration
$ACR_NAME = "musicstreamappnewdevacr"
$RESOURCE_GROUP = "musicstreamapp-new-dev-rg"
$LOCATION = "eastus"
$CONTAINER_NAME = "event-generator"
$TIMESTAMP = Get-Date -Format "yyyyMMddHHmmss"
$IMAGE_TAG = "v$TIMESTAMP"
$IMAGE_NAME = "$ACR_NAME.azurecr.io/event-generator:$IMAGE_TAG"
# Single Kafka broker with external IP (LoadBalancer service)
$KAFKA_BOOTSTRAP_SERVERS = "172.171.33.73:9092"

# Build and push the Docker image with the new tag
Write-Host "Building and pushing Docker image with tag $IMAGE_TAG..." -ForegroundColor Green
docker build -t $IMAGE_NAME -f Dockerfile ..
az acr login --name $ACR_NAME
docker push $IMAGE_NAME

# Ensure we're logged in to Azure
Write-Host "Ensuring Azure CLI login..." -ForegroundColor Green
az account show | Out-Null
if ($LASTEXITCODE -ne 0) {
    Write-Host "Not logged in to Azure. Please login..." -ForegroundColor Yellow
    az login
}

# Get ACR credentials
Write-Host "Getting ACR credentials..." -ForegroundColor Green
try {
    $ACR_USERNAME = az acr credential show --name $ACR_NAME --query "username" -o tsv
    $ACR_PASSWORD = az acr credential show --name $ACR_NAME --query "passwords[0].value" -o tsv
    
    if ([string]::IsNullOrEmpty($ACR_USERNAME) -or [string]::IsNullOrEmpty($ACR_PASSWORD)) {
        throw "Failed to retrieve valid ACR credentials"
    }
    
    Write-Host "Successfully retrieved ACR credentials" -ForegroundColor Green
}
catch {
    Write-Host "Error retrieving ACR credentials: $_" -ForegroundColor Red
    Write-Host "Attempting to use access token instead..." -ForegroundColor Yellow
    
    $TOKEN = az acr login --name $ACR_NAME --expose-token --query "accessToken" -o tsv
    $ACR_USERNAME = "00000000-0000-0000-0000-000000000000"
    $ACR_PASSWORD = $TOKEN
}

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