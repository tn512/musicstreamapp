output "id" {
  description = "The ID of the Container Registry"
  value       = azurerm_container_registry.main.id
}

output "login_server" {
  description = "The login server URL for the Container Registry"
  value       = azurerm_container_registry.main.login_server
}

output "admin_username" {
  description = "The admin username for the Container Registry"
  value       = azurerm_container_registry.main.admin_username
}

output "admin_password" {
  description = "The admin password for the Container Registry"
  value       = azurerm_container_registry.main.admin_password
  sensitive   = true
} 