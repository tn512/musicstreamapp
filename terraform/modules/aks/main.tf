resource "azurerm_kubernetes_cluster" "main" {
  name                = var.name
  location            = var.location
  resource_group_name = var.resource_group_name
  dns_prefix          = var.name
  
  default_node_pool {
    name           = "default"
    node_count     = var.node_count
    vm_size        = var.node_size
    vnet_subnet_id = var.subnet_id
  }

  network_profile {
    network_plugin = "azure"
    network_policy = "azure"
    service_cidr   = "172.16.0.0/16"  # Non-overlapping service CIDR
    dns_service_ip = "172.16.0.10"    # Must be within service_cidr
  }

  identity {
    type = "SystemAssigned"
  }

  tags = var.tags
} 