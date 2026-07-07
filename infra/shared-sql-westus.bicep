// Shared class Azure SQL — westus (explicit region exception for cross-region ADF lab).
// Teaches: SQL linked service, Copy SQL ↔ ADLS, while lake + ADF stay in eastus.
// Deploy: shared-adf-lab/orchestrate.cmd (idempotent INCREMENTAL).
targetScope = 'resourceGroup'

@description('westus — SQL only; documented exception (ADF cross-region pattern).')
@allowed(['westus'])
param location string = 'westus'

@description('Same hash as shared-eastus.bicep (uniqueString RG + shared + shared-eastus).')
param nameHash string

@description('Key Vault in eastus — stores SQL admin password.')
param keyVaultName string

@description('SQL administrator login (no @domain).')
param sqlAdminLogin string = 'finledgeradmin'

@secure()
@description('SQL administrator password — stored in Key Vault on first deploy.')
param sqlAdminPassword string

@description('Owner email for tags.')
param ownerEmail string

var tags = {
  env: 'training'
  owner: ownerEmail
  costcentre: 'boe-data-enablement'
  'data-class': 'training-synthetic'
  course: 'azure-etl-boe'
  class: 'shared-adf-sql-westus'
  estate: 'shared'
}

var sqlServerName = 'sql-shared-${nameHash}'
var sqlDatabaseName = 'finledger'

// ── Azure SQL logical server (westus) ───────────────────────────────────────
resource sqlServer 'Microsoft.Sql/servers@2023-08-01-preview' = {
  name: sqlServerName
  location: location
  tags: tags
  properties: {
    administratorLogin: sqlAdminLogin
    administratorLoginPassword: sqlAdminPassword
    version: '12.0'
    minimalTlsVersion: '1.2'
    publicNetworkAccess: 'Enabled'
  }
}

// Allow Azure services (ADF integration runtime) to reach SQL
resource firewallAzure 'Microsoft.Sql/servers/firewallRules@2023-08-01-preview' = {
  parent: sqlServer
  name: 'AllowAllAzureIPs'
  properties: {
    startIpAddress: '0.0.0.0'
    endIpAddress: '0.0.0.0'
  }
}

// ── Database — Basic tier (cheapest viable; training only) ───────────────────
resource sqlDatabase 'Microsoft.Sql/servers/databases@2023-08-01-preview' = {
  parent: sqlServer
  name: sqlDatabaseName
  location: location
  tags: tags
  sku: {
    name: 'Basic'
    tier: 'Basic'
  }
  properties: {
    collation: 'SQL_Latin1_General_CP1_CI_AS'
    maxSizeBytes: 2147483648 // 2 GB Basic limit
  }
}

// Store password in existing shared Key Vault (eastus)
resource kv 'Microsoft.KeyVault/vaults@2023-07-01' existing = {
  name: keyVaultName
}

resource sqlPasswordSecret 'Microsoft.KeyVault/vaults/secrets@2023-07-01' = {
  parent: kv
  name: 'sql-admin-password'
  properties: {
    value: sqlAdminPassword
  }
}

output sqlServerName string = sqlServer.name
output sqlServerFqdn string = sqlServer.properties.fullyQualifiedDomainName
output sqlDatabaseName string = sqlDatabase.name
output sqlAdminLogin string = sqlAdminLogin
output sqlLocation string = location
