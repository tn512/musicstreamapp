locals {
  resource_prefix = "${var.project}-${var.environment}"
  common_tags = merge(var.tags, {
    project     = var.project
    environment = var.environment
  })
}

module "resource_group" {
  source = "../modules/rg"
  
  name     = "${local.resource_prefix}-rg"
  location = var.location  # This will use eastus from variables
  tags     = local.common_tags
}

module "databricks_resource_group" {
  source = "../modules/rg"
  
  name     = "${local.resource_prefix}-databricks-rg"
  location = "westeurope"
  tags     = local.common_tags
}

module "vnet" {
  source = "../modules/vnet"
  
  name                = "${local.resource_prefix}-vnet"
  resource_group_name = module.resource_group.name
  location           = module.resource_group.location
  address_space      = [var.address_space]
  aks_subnet_prefix  = var.aks_subnet_prefix
  databricks_public_subnet_prefix  = var.databricks_public_subnet_prefix
  databricks_private_subnet_prefix = var.databricks_private_subnet_prefix
  tags               = local.common_tags
}

module "databricks_vnet" {
  source = "../modules/vnet"
  
  name                = "${local.resource_prefix}-databricks-vnet"
  resource_group_name = module.databricks_resource_group.name
  location           = module.databricks_resource_group.location
  address_space      = ["10.1.0.0/16"]  # Different address space for Databricks
  aks_subnet_prefix  = "10.1.1.0/24"    # Not used but required by module
  databricks_public_subnet_prefix  = "10.1.2.0/24"
  databricks_private_subnet_prefix = "10.1.3.0/24"
  tags               = local.common_tags
}

module "aks" {
  source = "../modules/aks"
  
  name                = "${local.resource_prefix}-aks"
  resource_group_name = module.resource_group.name
  location           = module.resource_group.location
  node_count         = var.aks_node_count
  node_size          = var.aks_node_size
  subnet_id          = module.vnet.aks_subnet_id
  tags               = local.common_tags
}

module "storage" {
  source = "../modules/storage"
  
  name                = "${replace(local.resource_prefix, "-", "")}dls"
  resource_group_name = module.resource_group.name
  location           = module.resource_group.location
  tags               = local.common_tags
}

module "databricks" {
  source = "../modules/databricks"
  
  name                = "${local.resource_prefix}-databricks"
  resource_group_name = module.databricks_resource_group.name
  location           = module.databricks_resource_group.location
  virtual_network_id = module.databricks_vnet.id
  public_subnet_name = module.databricks_vnet.databricks_public_subnet_name
  private_subnet_name = module.databricks_vnet.databricks_private_subnet_name
  public_subnet_nsg_association_id = module.databricks_vnet.databricks_public_nsg_association_id
  private_subnet_nsg_association_id = module.databricks_vnet.databricks_private_nsg_association_id
  tags               = local.common_tags
}

module "key_vault" {
  source = "../modules/keyvault"
  
  name                = "${replace(local.resource_prefix, "-", "")}kv"
  resource_group_name = module.resource_group.name
  location           = module.resource_group.location
  tags               = local.common_tags
}

module "container_registry" {
  source = "../modules/acr"
  
  name                = "${replace(local.resource_prefix, "-", "")}acr"
  resource_group_name = module.resource_group.name
  location           = module.resource_group.location
  aks_principal_id   = module.aks.principal_id
  tags               = local.common_tags
} 