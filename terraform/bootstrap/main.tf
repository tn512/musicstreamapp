provider "azurerm" {
  features {}
}

resource "azurerm_resource_group" "tfstate" {
  name     = "musicstream-tfstate-rg"
  location = "eastus"
  
  tags = {
    environment = "tfstate"
    managed_by  = "terraform"
  }
}

resource "azurerm_storage_account" "tfstate" {
  name                     = "musicstreamtfstate"
  resource_group_name      = azurerm_resource_group.tfstate.name
  location                 = azurerm_resource_group.tfstate.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  min_tls_version         = "TLS1_2"

  tags = {
    environment = "tfstate"
    managed_by  = "terraform"
  }
}

resource "azurerm_storage_container" "tfstate" {
  name                  = "tfstate"
  storage_account_name  = azurerm_storage_account.tfstate.name
  container_access_type = "private"
}

# Optional: Add a role assignment for your user/service principal
data "azurerm_client_config" "current" {}

resource "azurerm_role_assignment" "tfstate_contributor" {
  scope                = azurerm_storage_account.tfstate.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = data.azurerm_client_config.current.object_id
} 