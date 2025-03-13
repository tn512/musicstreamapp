output "id" {
  description = "The ID of the virtual network"
  value       = azurerm_virtual_network.main.id
}

output "name" {
  description = "The name of the virtual network"
  value       = azurerm_virtual_network.main.name
}

output "aks_subnet_id" {
  description = "The ID of the AKS subnet"
  value       = azurerm_subnet.aks.id
}

output "databricks_public_subnet_name" {
  description = "The name of the Databricks public subnet"
  value       = azurerm_subnet.databricks_public.name
}

output "databricks_private_subnet_name" {
  description = "The name of the Databricks private subnet"
  value       = azurerm_subnet.databricks_private.name
}

output "databricks_public_nsg_association_id" {
  description = "The ID of the public subnet NSG association"
  value       = azurerm_subnet_network_security_group_association.databricks_public.id
}

output "databricks_private_nsg_association_id" {
  description = "The ID of the private subnet NSG association"
  value       = azurerm_subnet_network_security_group_association.databricks_private.id
} 