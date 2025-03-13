output "id" {
  description = "The ID of the Key Vault"
  value       = azurerm_key_vault.main.id
}

output "vault_uri" {
  description = "The URI of the Key Vault"
  value       = azurerm_key_vault.main.vault_uri
} 