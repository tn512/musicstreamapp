variable "name" {
  description = "Name of the virtual network"
  type        = string
}

variable "location" {
  description = "Azure region"
  type        = string
}

variable "resource_group_name" {
  description = "Name of the resource group"
  type        = string
}

variable "address_space" {
  description = "Address space for the virtual network"
  type        = list(string)
}

variable "aks_subnet_prefix" {
  description = "Address prefix for the AKS subnet"
  type        = string
}

variable "databricks_public_subnet_prefix" {
  description = "Address prefix for the Databricks public subnet"
  type        = string
}

variable "databricks_private_subnet_prefix" {
  description = "Address prefix for the Databricks private subnet"
  type        = string
}

variable "tags" {
  description = "Tags to apply to resources"
  type        = map(string)
  default     = {}
} 