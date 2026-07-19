# Terraform twin of infra/shared-sql-westus.bicep (SQL Basic in westus).
# Deploy after shared-eastus (needs Key Vault name + name_hash).
# Usually invoked from shared-adf-lab; this module is for IaC teaching parity.

terraform {
  required_version = ">= 1.5.0"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.117"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
}

provider "azurerm" {
  features {}
  subscription_id = var.subscription_id
}

variable "subscription_id" { type = string }
variable "resource_group_name" {
  type    = string
  default = "rg-shared-class1"
}
variable "name_hash" {
  type        = string
  description = "Same hash as shared-eastus (e.g. qgr7mj)."
}
variable "key_vault_name" { type = string }
variable "owner_email" { type = string }
variable "sql_admin_login" {
  type    = string
  default = "finledgeradmin"
}
variable "sql_admin_password" {
  type      = string
  sensitive = true
  default   = ""
}

locals {
  tags = {
    env          = "training"
    owner        = var.owner_email
    costcentre   = "boe-data-enablement"
    "data-class" = "training-synthetic"
    course       = "azure-etl-boe"
    class        = "shared-adf-sql-westus"
    estate       = "shared"
    iac          = "terraform"
  }
  sql_server_name   = "sql-shared-${var.name_hash}"
  sql_database_name = "finledger"
}

resource "random_password" "sql" {
  count   = var.sql_admin_password == "" ? 1 : 0
  length  = 24
  special = true
}

locals {
  sql_password = var.sql_admin_password != "" ? var.sql_admin_password : random_password.sql[0].result
}

data "azurerm_resource_group" "shared" {
  name = var.resource_group_name
}

data "azurerm_key_vault" "shared" {
  name                = var.key_vault_name
  resource_group_name = var.resource_group_name
}

resource "azurerm_mssql_server" "shared" {
  name                          = local.sql_server_name
  resource_group_name           = data.azurerm_resource_group.shared.name
  location                      = "westus"
  version                       = "12.0"
  administrator_login           = var.sql_admin_login
  administrator_login_password  = local.sql_password
  minimum_tls_version           = "1.2"
  public_network_access_enabled = true
  tags                          = local.tags
}

resource "azurerm_mssql_firewall_rule" "azure_services" {
  name             = "AllowAllAzureIPs"
  server_id        = azurerm_mssql_server.shared.id
  start_ip_address = "0.0.0.0"
  end_ip_address   = "0.0.0.0"
}

resource "azurerm_mssql_database" "finledger" {
  name        = local.sql_database_name
  server_id   = azurerm_mssql_server.shared.id
  sku_name    = "Basic"
  max_size_gb = 2
  collation   = "SQL_Latin1_General_CP1_CI_AS"
  tags        = local.tags
}

resource "azurerm_key_vault_secret" "sql_password" {
  name         = "sql-admin-password"
  value        = local.sql_password
  key_vault_id = data.azurerm_key_vault.shared.id
}

output "sqlServerName" { value = azurerm_mssql_server.shared.name }
output "sqlServerFqdn" { value = azurerm_mssql_server.shared.fully_qualified_domain_name }
output "sqlDatabaseName" { value = azurerm_mssql_database.finledger.name }
output "sqlAdminLogin" { value = var.sql_admin_login }
output "sqlLocation" { value = "westus" }
