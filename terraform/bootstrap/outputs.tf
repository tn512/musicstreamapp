output "storage_account_name" {
  value = azurerm_storage_account.tfstate.name
}

output "container_name" {
  value = azurerm_storage_container.tfstate.name
}

output "resource_group_name" {
  value = azurerm_resource_group.tfstate.name
}

output "access_key" {
  value     = azurerm_storage_account.tfstate.primary_access_key
  sensitive = true
} 