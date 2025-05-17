terraform {
  backend "azurerm" {
    resource_group_name  = "musicstream-tfstate-rg-final"
    storage_account_name = "musicstreamtfstatefinal"
    container_name      = "tfstate"
    key                 = "terraform.tfstate"
  }
} 