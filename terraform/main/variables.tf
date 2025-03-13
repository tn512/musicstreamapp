variable "project" {
  description = "Project name"
  type        = string
}

variable "environment" {
  description = "Environment (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "location" {
  description = "Azure region"
  type        = string
  default     = "eastus"
}

variable "address_space" {
  description = "VNet address space"
  type        = string
  default     = "10.0.0.0/16"
}

variable "aks_subnet_prefix" {
  description = "AKS subnet address prefix"
  type        = string
  default     = "10.0.1.0/24"
}

variable "databricks_public_subnet_prefix" {
  description = "Databricks public subnet address prefix"
  type        = string
  default     = "10.0.2.0/24"
}

variable "databricks_private_subnet_prefix" {
  description = "Databricks private subnet address prefix"
  type        = string
  default     = "10.0.3.0/24"
}

variable "aks_node_count" {
  description = "Number of AKS nodes"
  type        = number
  default     = 1
}

variable "aks_node_size" {
  description = "Size of AKS nodes"
  type        = string
  default     = "Standard_D4s_v3"
}

variable "tags" {
  description = "Tags to apply to all resources"
  type        = map(string)
  default     = {}
} 