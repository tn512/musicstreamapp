terraform {
  backend "azurerm" {
    resource_group_name  = "musicstream-tfstate-rg"
    storage_account_name = "musicstreamtfstate"
    container_name      = "tfstate"
    key                 = "terraform.tfstate"
  }
} 