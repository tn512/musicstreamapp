terraform {
  backend "azurerm" {
    resource_group_name  = "musicstream-tfstate-rg-p02"
    storage_account_name = "musicstreamtfstatep02"
    container_name      = "tfstate"
    key                 = "terraform.tfstate"
  }
} 