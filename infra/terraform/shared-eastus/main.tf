# Shared student estate — Terraform twin of infra/shared-eastus.bicep
# Same resources, same tags, same region exception (eastus).
# Deploy: provision-shared-tf.cmd   |   Compare: docs/BICEP-TERRAFORM-SHARED-ESTATE.md

terraform {
  required_version = ">= 1.5.0"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.117"
    }
    azuread = {
      source  = "hashicorp/azuread"
      version = "~> 2.53"
    }
  }
}

provider "azurerm" {
  features {
    key_vault {
      purge_soft_delete_on_destroy    = false
      recover_soft_deleted_key_vaults = true
    }
    resource_group {
      prevent_deletion_if_contains_resources = true
    }
  }
  subscription_id = var.subscription_id
}

# ---------------------------------------------------------------------------
# Inputs (mirror Bicep params)
# ---------------------------------------------------------------------------
# name_hash: Bicep uses uniqueString(rg.id, learner, 'shared-eastus')[0:6].
# Terraform cannot reproduce Azure uniqueString bit-for-bit. For the LIVE class
# estate set name_hash = "qgr7mj" (from stsharedqgr7mj). For a greenfield TF-only
# estate, leave empty to use a deterministic sha1-based hash (new names).
# ---------------------------------------------------------------------------

variable "subscription_id" {
  type        = string
  description = "Azure subscription ID (same as AZURE_SUBSCRIPTION_ID / Bicep deploy)."
}

variable "location" {
  type        = string
  description = "eastus — shared training subscription (documented region exception)."
  default     = "eastus"
  validation {
    condition     = var.location == "eastus"
    error_message = "Shared estate location must be eastus (documented exception)."
  }
}

variable "learner" {
  type        = string
  description = "Shared identifier for names (Bicep param learner)."
  default     = "shared"
}

variable "owner_email" {
  type        = string
  description = "Owner email for tags (Bicep ownerEmail)."
}

variable "principal_object_id" {
  type        = string
  description = "Object ID for data-plane RBAC (Bicep principalObjectId)."
}

variable "principal_type" {
  type        = string
  description = "User or ServicePrincipal (Bicep principalType)."
  default     = "User"
  validation {
    condition     = contains(["User", "ServicePrincipal"], var.principal_type)
    error_message = "principal_type must be User or ServicePrincipal."
  }
}

variable "resource_group_name" {
  type        = string
  description = "RG name (Bicep targetScope resourceGroup — created by provision_shared)."
  default     = "rg-shared-class1"
}

variable "name_hash" {
  type        = string
  description = "6-char hash matching Bicep uniqueString slice. Empty = sha1-derived (new names)."
  default     = ""
}

variable "deploy_budget" {
  type        = bool
  description = "Bicep deployBudget — false on Sponsorship (Cost Management budgets blocked)."
  default     = false
}

variable "budget_start_date" {
  type        = string
  description = "YYYY-MM-01 budget anchor (Bicep budgetStartDate)."
  default     = ""
}

# ---------------------------------------------------------------------------
# Locals — same naming scheme as Bicep
# ---------------------------------------------------------------------------

locals {
  # Prefer explicit hash (parity with existing Bicep estate); else deterministic TF hash
  name_hash = var.name_hash != "" ? var.name_hash : substr(sha1("${var.resource_group_name}|${var.learner}|shared-eastus"), 0, 6)

  storage_account_name = lower("st${var.learner}${local.name_hash}")
  key_vault_name       = "kv-${var.learner}-${local.name_hash}"
  adf_name             = "adf-${var.learner}-${local.name_hash}"
  databricks_name      = "dbw-${var.learner}-${local.name_hash}"
  managed_rg_name      = "rg-${var.learner}-dbw-${local.name_hash}"

  tags = {
    env             = "training"
    owner           = var.owner_email
    costcentre      = "boe-data-enablement"
    "data-class"    = "training-synthetic"
    course          = "azure-etl-boe"
    class           = "shared-eastus"
    "auto-teardown" = "manual"
    estate          = "shared"
    iac             = "terraform"
  }

  budget_start = var.budget_start_date != "" ? var.budget_start_date : formatdate("YYYY-MM-01", timestamp())

  # Role definition IDs (same GUIDs as Bicep)
  kv_secrets_officer_role_id            = "b86a8fe4-44ce-4948-aee5-eccb2c155cd7"
  storage_blob_data_contributor_role_id = "ba92f5b4-2d11-453d-a403-e96b0029c9fe"

  container_names = ["bronze", "silver", "gold", "audit"]
}

# ---------------------------------------------------------------------------
# Resource group (Bicep assumes RG already exists; TF can create it)
# ---------------------------------------------------------------------------

resource "azurerm_resource_group" "shared" {
  name     = var.resource_group_name
  location = var.location
  tags     = local.tags
}

# ---------------------------------------------------------------------------
# Optional budget (Bicep Microsoft.Consumption/budgets)
# ---------------------------------------------------------------------------

resource "azurerm_consumption_budget_resource_group" "shared" {
  count = var.deploy_budget ? 1 : 0

  name              = "budget-${var.learner}-shared"
  resource_group_id = azurerm_resource_group.shared.id
  amount            = 50
  time_grain        = "Monthly"

  time_period {
    start_date = "${local.budget_start}T00:00:00Z"
    end_date   = "2030-12-31T00:00:00Z"
  }

  notification {
    enabled        = true
    threshold      = 50
    operator       = "GreaterThan"
    threshold_type = "Actual"
    contact_emails = [var.owner_email]
  }

  notification {
    enabled        = true
    threshold      = 90
    operator       = "GreaterThan"
    threshold_type = "Forecasted"
    contact_emails = [var.owner_email]
  }
}

# ---------------------------------------------------------------------------
# Key Vault (Bicep Microsoft.KeyVault/vaults)
# ---------------------------------------------------------------------------

data "azurerm_client_config" "current" {}

resource "azurerm_key_vault" "shared" {
  name                            = local.key_vault_name
  location                        = azurerm_resource_group.shared.location
  resource_group_name             = azurerm_resource_group.shared.name
  tenant_id                       = data.azurerm_client_config.current.tenant_id
  sku_name                        = "standard"
  soft_delete_retention_days      = 7
  purge_protection_enabled        = false
  enable_rbac_authorization       = true
  enabled_for_deployment          = false
  enabled_for_disk_encryption     = false
  enabled_for_template_deployment = false
  public_network_access_enabled   = true
  tags                            = local.tags
}

resource "azurerm_role_assignment" "kv_secrets_officer" {
  scope                = azurerm_key_vault.shared.id
  role_definition_name = "Key Vault Secrets Officer"
  principal_id         = var.principal_object_id
  # principal_type not required when using object id; TF resolves
}

# ---------------------------------------------------------------------------
# ADLS Gen2 (Bicep Microsoft.Storage/storageAccounts + containers + lifecycle)
# ---------------------------------------------------------------------------

resource "azurerm_storage_account" "shared" {
  name                            = local.storage_account_name
  resource_group_name             = azurerm_resource_group.shared.name
  location                        = azurerm_resource_group.shared.location
  account_tier                    = "Standard"
  account_replication_type        = "LRS"
  account_kind                    = "StorageV2"
  access_tier                     = "Hot"
  is_hns_enabled                  = true
  https_traffic_only_enabled      = true
  min_tls_version                 = "TLS1_2"
  allow_nested_items_to_be_public = false
  tags                            = local.tags

  blob_properties {
    delete_retention_policy {
      days = 7
    }
    container_delete_retention_policy {
      days = 7
    }
  }
}

resource "azurerm_storage_container" "medallion" {
  for_each              = toset(local.container_names)
  name                  = each.value
  storage_account_name  = azurerm_storage_account.shared.name
  container_access_type = "private"
}

resource "azurerm_storage_management_policy" "lifecycle" {
  storage_account_id = azurerm_storage_account.shared.id

  rule {
    name    = "tier-block-blobs-cool-30d"
    enabled = true
    filters {
      blob_types = ["blockBlob"]
    }
    actions {
      base_blob {
        tier_to_cool_after_days_since_modification_greater_than = 30
      }
    }
  }

  # Mirrors Bicep delete-block-blobs-7d (training cleanup rule)
  rule {
    name    = "delete-block-blobs-7d"
    enabled = true
    filters {
      blob_types = ["blockBlob"]
    }
    actions {
      base_blob {
        delete_after_days_since_modification_greater_than = 7
      }
    }
  }
}

resource "azurerm_role_assignment" "storage_blob_contributor" {
  scope                = azurerm_storage_account.shared.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = var.principal_object_id
}

# ---------------------------------------------------------------------------
# Azure Data Factory (Bicep Microsoft.DataFactory/factories + linked service)
# ---------------------------------------------------------------------------

resource "azurerm_data_factory" "shared" {
  name                = local.adf_name
  location            = azurerm_resource_group.shared.location
  resource_group_name = azurerm_resource_group.shared.name
  tags                = local.tags

  identity {
    type = "SystemAssigned"
  }

  public_network_enabled = true
}

resource "azurerm_data_factory_linked_service_data_lake_storage_gen2" "bronze" {
  name                 = "AdlsBronzeLinkedService"
  data_factory_id      = azurerm_data_factory.shared.id
  url                  = azurerm_storage_account.shared.primary_dfs_endpoint
  use_managed_identity = true
}

# ADF MSI needs Blob Data Contributor (Bicep LS was URL-only; TF makes auth complete)
resource "azurerm_role_assignment" "adf_storage" {
  scope                = azurerm_storage_account.shared.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_data_factory.shared.identity[0].principal_id
}

# ---------------------------------------------------------------------------
# Databricks (Bicep Microsoft.Databricks/workspaces)
# ---------------------------------------------------------------------------

resource "azurerm_databricks_workspace" "shared" {
  name                          = local.databricks_name
  resource_group_name           = azurerm_resource_group.shared.name
  location                      = azurerm_resource_group.shared.location
  sku                           = "premium"
  managed_resource_group_name   = local.managed_rg_name
  public_network_access_enabled = true
  tags                          = local.tags

  custom_parameters {
    no_public_ip = false
  }
}

# ---------------------------------------------------------------------------
# Outputs (same names as Bicep outputs)
# ---------------------------------------------------------------------------

output "storageAccountName" {
  value = azurerm_storage_account.shared.name
}

output "keyVaultName" {
  value = azurerm_key_vault.shared.name
}

output "dfsEndpoint" {
  value = azurerm_storage_account.shared.primary_dfs_endpoint
}

output "dataFactoryName" {
  value = azurerm_data_factory.shared.name
}

output "databricksWorkspaceName" {
  value = azurerm_databricks_workspace.shared.name
}

output "databricksManagedResourceGroup" {
  value = local.managed_rg_name
}

output "location" {
  value = var.location
}

output "resourceGroupName" {
  value = azurerm_resource_group.shared.name
}

output "nameHash" {
  value       = local.name_hash
  description = "Hash used in names — set name_hash=qgr7mj to match live Bicep estate."
}
