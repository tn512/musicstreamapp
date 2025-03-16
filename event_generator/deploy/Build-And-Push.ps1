# Configuration
$ACR_NAME = "musicstreamappdevacr"
$IMAGE_NAME = "event-generator"
$TAG = "latest"

# Log in to Azure Container Registry
Write-Host "Logging in to Azure Container Registry..." -ForegroundColor Green
az acr login --name $ACR_NAME

# Build the Docker image
Write-Host "Building Docker image..." -ForegroundColor Green
docker build -t "$ACR_NAME.azurecr.io/$IMAGE_NAME`:$TAG" -f Dockerfile ..

# Push the image to ACR
Write-Host "Pushing image to ACR..." -ForegroundColor Green
docker push "$ACR_NAME.azurecr.io/$IMAGE_NAME`:$TAG"

Write-Host "Image successfully built and pushed to ACR: $ACR_NAME.azurecr.io/$IMAGE_NAME`:$TAG" -ForegroundColor Green 