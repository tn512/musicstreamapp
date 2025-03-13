variable "name" {
  description = "Name of the Databricks workspace"
  type        = string
}

variable "resource_group_name" {
  description = "Name of the resource group"
  type        = string
}

variable "location" {
  description = "Azure region"
  type        = string
}

variable "virtual_network_id" {
  description = "ID of the virtual network where Databricks will be deployed"
  type        = string
}

variable "public_subnet_name" {
  description = "Name of the public subnet for Databricks"
  type        = string
}

variable "private_subnet_name" {
  description = "Name of the private subnet for Databricks"
  type        = string
}

variable "public_subnet_nsg_association_id" {
  description = "ID of the public subnet's NSG association"
  type        = string
}

variable "private_subnet_nsg_association_id" {
  description = "ID of the private subnet's NSG association"
  type        = string
}

variable "tags" {
  description = "Tags to apply to resources"
  type        = map(string)
  default     = {}
} 