terraform {
  backend "azurerm" {
    resource_group_name  = "musicstream-tfstate-rg-new"
    storage_account_name = "musicstreamtfstatenew"
    container_name      = "tfstate"
    key                 = "terraform.tfstate"
  }
} 