output "id" {
  description = "The ID of the storage account"
  value       = azurerm_storage_account.main.id
}

output "name" {
  description = "The name of the storage account"
  value       = azurerm_storage_account.main.name
}

output "primary_access_key" {
  description = "The primary access key for the storage account"
  value       = azurerm_storage_account.main.primary_access_key
  sensitive   = true
}

output "raw_container_name" {
  description = "The name of the raw data container"
  value       = azurerm_storage_container.raw.name
}

output "processed_container_name" {
  description = "The name of the processed data container"
  value       = azurerm_storage_container.processed.name
}

output "delta_container_name" {
  description = "The name of the delta lake container"
  value       = azurerm_storage_container.delta.name
} 