# Infrastructure as Code (Terraform)

This directory contains the Terraform configurations for provisioning Azure infrastructure for the Music Streaming Analytics project.

## Directory Structure

```
terraform/
├── bootstrap/           # Bootstrap configuration for Terraform state
│   ├── main.tf         # Main bootstrap configuration
│   ├── variables.tf    # Bootstrap variables
│   └── outputs.tf      # Bootstrap outputs
├── main/               # Main infrastructure configuration
│   ├── main.tf         # Main infrastructure setup
│   ├── variables.tf    # Main variables
│   ├── outputs.tf      # Main outputs
│   └── terraform.tfvars # Variable values
└── modules/            # Reusable Terraform modules
    ├── aks/           # AKS cluster module
    ├── databricks/    # Databricks workspace module
    ├── keyvault/      # Key Vault module
    ├── rg/            # Resource Group module
    ├── storage/       # Storage Account module
    └── vnet/          # Virtual Network module
```

## Resource Groups

The infrastructure creates the following resource groups:

1. `musicstream-tfstate-rg-new`
   - Purpose: Stores Terraform state files
   - Location: eastus
   - Resources:
     - Storage Account for Terraform state

2. `musicstreamapp-dev-rg`
   - Purpose: Main infrastructure resources
   - Location: eastus
   - Resources:
     - AKS cluster
     - Virtual Network
     - Storage Account (Data Lake)
     - Key Vault
     - Container Registry

3. `musicstreamapp-dev-databricks-rg`
   - Purpose: Databricks workspace
   - Location: westeurope
   - Resources:
     - Databricks workspace
     - Databricks Virtual Network

4. `MC_musicstreamapp-dev-rg_musicstreamapp-dev-aks_eastus`
   - Purpose: AKS-managed resources
   - Location: eastus
   - Resources:
     - AKS node VMs
     - Network interfaces
     - Disks

## Setup Instructions

1. **Prerequisites**
   ```bash
   # Install Azure CLI
   # Install Terraform
   # Login to Azure
   az login
   
   # Set your active subscription (replace with your subscription ID)
   az account set --subscription "YOUR_SUBSCRIPTION_ID"
   ```

2. **Bootstrap Setup**
   ```bash
   cd bootstrap
   terraform init
   terraform plan
   terraform apply
   ```

3. **Main Infrastructure Setup**
   ```bash
   cd ../main
   terraform init
   terraform plan
   terraform apply
   ```

## Switching Azure Accounts

If you need to move your infrastructure to a different Azure account:

1. **Update Project Name in terraform.tfvars**
   ```hcl
   # In main/terraform.tfvars
   project = "musicstreamapp-final"  # Update this with your new project name
   environment = "dev"               # Keep or change based on your needs
   ```

2. **Update Bootstrap Configuration**
   ```hcl
   # In bootstrap/main.tf
   resource "azurerm_resource_group" "tfstate" {
     name     = "musicstream-tfstate-rg-final"      # Update with new name
     location = "eastus"
   }

   resource "azurerm_storage_account" "tfstate" {
     name                     = "musicstreamtfstatefinal"  # Update with new name
     resource_group_name      = azurerm_resource_group.tfstate.name
     # ... rest of configuration
   }
   ```

3. **Update Backend Configuration**
   ```hcl
   # In main/backend.tf
   terraform {
     backend "azurerm" {
       resource_group_name  = "musicstream-tfstate-rg-final"     # Match bootstrap RG name
       storage_account_name = "musicstreamtfstatefinal"          # Match bootstrap storage name
       container_name      = "tfstate"
       key                 = "terraform.tfstate"
     }
   }
   ```

4. **Resource Naming Changes**
   The following resources will be automatically renamed based on your project name change:
   - Resource Groups:
     - Main: `musicstreamapp-final-dev-rg`
     - Databricks: `musicstreamapp-final-dev-databricks-rg`
   - Storage Account: `musicstreamappfinaldls`
   - Key Vault: `musicstreamappfinaldevkv`
   - Container Registry: `musicstreamappfinaldevacr`
   - AKS Cluster: `musicstreamapp-final-dev-aks`
   - Databricks Workspace: `musicstreamapp-final-dev-databricks`
   - Virtual Networks:
     - Main: `musicstreamapp-final-dev-vnet`
     - Databricks: `musicstreamapp-final-dev-databricks-vnet`

5. **Reinitialize and Apply**
   ```bash
   # First, apply bootstrap configuration
   cd bootstrap
   terraform init
   terraform plan
   terraform apply

   # Then, initialize and apply main configuration
   cd ../main
   terraform init -reconfigure
   terraform plan
   terraform apply
   ```

6. **Verify Resources**
   After applying the configuration, verify the resources are created with the new names:
   ```bash
   # List resource groups
   az group list --query "[].name" -o table

   # Check AKS cluster
   az aks show --resource-group musicstreamapp-final-dev-rg --name musicstreamapp-final-dev-aks

   # Verify storage account
   az storage account show --resource-group musicstreamapp-final-dev-rg --name musicstreamappfinaldls
   ```

## Testing

1. **Verify Resource Groups**
   ```bash
   az group list --query "[].{Name:name, Location:location}" -o table
   ```

2. **Verify AKS Cluster**
   ```bash
   az aks get-credentials --resource-group musicstreamapp-dev-rg --name musicstreamapp-dev-aks
   kubectl get nodes
   ```

3. **Verify Databricks Workspace**
   ```bash
   az databricks workspace show --resource-group musicstreamapp-dev-databricks-rg --name musicstreamapp-dev-databricks
   ```

4. **Verify Storage Account**
   ```bash
   az storage account show --resource-group musicstreamapp-dev-rg --name musicstreamappdevdls
   ```

## Maintenance

1. **Updating Infrastructure**
   ```bash
   cd main
   terraform plan
   terraform apply
   ```

2. **Destroying Infrastructure**
   ```bash
   terraform destroy
   ```

## Security Considerations

1. **Key Vault**
   - Stores sensitive information
   - Access policies for AKS and Databricks
   - Network access restrictions

2. **Network Security**
   - Private endpoints where possible
   - Network security groups
   - Subnet configurations

3. **Identity Management**
   - Managed identities for AKS and Databricks
   - Role-based access control (RBAC)

## Troubleshooting

1. **Common Issues**
   - Resource naming conflicts
   - Network connectivity issues
   - Permission problems
   - Authentication failures

2. **Solutions**
   - Check resource naming conventions
   - Verify network configurations
   - Review RBAC assignments
   - Ensure proper authentication with `az login` and `az account set`
   - Set environment variables if needed:
     ```powershell
     $env:ARM_SUBSCRIPTION_ID="YOUR_SUBSCRIPTION_ID"
     $env:ARM_TENANT_ID="YOUR_TENANT_ID"
     ```

## Best Practices

1. **State Management**
   - Use remote state storage
   - Enable state locking
   - Regular state backups

2. **Resource Naming**
   - Follow consistent naming convention
   - Include environment tags
   - Use meaningful prefixes

3. **Module Usage**
   - Reuse existing modules
   - Keep modules generic
   - Version control modules

## References

- [Azure Provider Documentation](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs)
- [Terraform Best Practices](https://www.terraform.io/docs/cloud/guides/recommended-practices/index.html)
- [Azure Architecture Center](https://docs.microsoft.com/en-us/azure/architecture/) 