output "id" {
  description = "The ID of the AKS cluster"
  value       = azurerm_kubernetes_cluster.main.id
}

output "kube_config" {
  description = "The kubeconfig for the AKS cluster"
  value       = azurerm_kubernetes_cluster.main.kube_config_raw
  sensitive   = true
}

output "principal_id" {
  description = "The principal ID of the AKS cluster's managed identity"
  value       = azurerm_kubernetes_cluster.main.identity[0].principal_id
} 