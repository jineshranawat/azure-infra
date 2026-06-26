// Class-1 platform services — ADF, Synapse (serverless SQL), Purview, Databricks.
// Deploy AFTER main.bicep (requires existing ADLS storage account).
// WARNING: Purview and Databricks may incur charges; ADF factory + Synapse workspace are low-idle.
targetScope = 'resourceGroup'

@description('UK-only region.')
@allowed(['uksouth', 'ukwest'])
param location string

@minLength(2)
@maxLength(10)
param learner string

param ownerEmail string

@description('Existing Class-1 storage account (ADLS Gen2).')
param storageAccountName string

@secure()
@description('Synapse workspace SQL admin password.')
param synapseSqlPassword string

param synapseSqlUser string = 'synapseadmin'

@description('Set false if subscription blocks new SQL/Synapse workspaces (common on MPN).')
param deploySynapse bool = false

@description('Synapse workspace region when deploySynapse is true.')
param synapseLocation string = 'uksouth'

@description('Deploy Microsoft Purview account (tenant allows one free tier — use purviewLocation eastus).')
param deployPurview bool = true

@description('Purview region (eastus when tenant free tier is in US).')
param purviewLocation string = 'eastus'

@description('Databricks workspace region (eastus2 — broader quota/SKU availability on training subscriptions).')
param databricksLocation string = 'eastus2'

@description('Deploy Microsoft Fabric capacity (F2 — billable).')
param deployFabric bool = true

@description('Fabric capacity region (Fabric not available in all UK regions).')
param fabricLocation string = 'westeurope'

@description('Fabric capacity admin UPNs.')
param fabricAdminMembers array = []

@description('Fabric capacity SKU name (F0 trial if quota allows).')
param fabricSkuName string = 'F0'

var tags = {
  env: 'training'
  owner: ownerEmail
  costcentre: 'boe-data-enablement'
  'data-class': 'training-synthetic'
  course: 'azure-etl-boe'
  class: 'class-1-platforms'
  'auto-teardown': 'nightly'
}

var nameHash = substring(uniqueString(resourceGroup().id, learner, 'platforms'), 0, 6)
var adfName = 'adf-${learner}-${nameHash}'
var synapseName = 'syn-${learner}-${nameHash}'
var purviewName = 'pview${learner}${nameHash}'
var databricksName = 'dbw-${learner}-${nameHash}'
var managedRgName = 'rg-${learner}-dbw-${nameHash}'
var fabricCapacityName = 'fc${learner}${nameHash}'

var storageBlobContributorRoleId = 'ba92f5b4-2d11-453d-a403-e96b0029c9fe'

resource storageAccount 'Microsoft.Storage/storageAccounts@2023-05-01' existing = {
  name: storageAccountName
}

// ── 1. Azure Data Factory (factory free; pay per pipeline / IR usage) ─────────
resource dataFactory 'Microsoft.DataFactory/factories@2018-06-01' = {
  name: adfName
  location: location
  tags: tags
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    publicNetworkAccess: 'Enabled'
  }
}

resource adfStorageLinkedService 'Microsoft.DataFactory/factories/linkedservices@2018-06-01' = {
  parent: dataFactory
  name: 'AdlsBronzeLinkedService'
  properties: {
    type: 'AzureBlobFS'
    typeProperties: {
      url: storageAccount.properties.primaryEndpoints.dfs
    }
    annotations: []
  }
}

// ── 2. Synapse Analytics workspace (optional — includes serverless SQL pool) ──
resource synapseWorkspace 'Microsoft.Synapse/workspaces@2021-06-01' = if (deploySynapse) {
  name: synapseName
  location: synapseLocation
  tags: tags
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    defaultDataLakeStorage: {
      accountUrl: storageAccount.properties.primaryEndpoints.dfs
      filesystem: 'bronze'
    }
    sqlAdministratorLogin: synapseSqlUser
    sqlAdministratorLoginPassword: synapseSqlPassword
    managedVirtualNetwork: 'default'
    publicNetworkAccess: 'Enabled'
    azureADOnlyAuthentication: false
  }
}

resource synapseFirewall 'Microsoft.Synapse/workspaces/firewallRules@2021-06-01' = if (deploySynapse) {
  parent: synapseWorkspace
  name: 'AllowAllWindowsAzureIps'
  properties: {
    startIpAddress: '0.0.0.0'
    endIpAddress: '0.0.0.0'
  }
}

resource synapseStorageRbac 'Microsoft.Authorization/roleAssignments@2022-04-01' = if (deploySynapse) {
  name: guid(storageAccount.id, synapseWorkspace.id, storageBlobContributorRoleId)
  scope: storageAccount
  properties: {
    roleDefinitionId: concat('/subscriptions/', subscription().subscriptionId, '/providers/Microsoft.Authorization/roleDefinitions/', storageBlobContributorRoleId)
    principalId: synapseWorkspace.identity.principalId
    principalType: 'ServicePrincipal'
  }
}

// ── 3. Microsoft Purview (optional — tenant may already have free tier in eastus) ─
resource purviewAccount 'Microsoft.Purview/accounts@2021-07-01' = if (deployPurview) {
  name: purviewName
  location: purviewLocation
  tags: tags
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    publicNetworkAccess: 'Enabled'
  }
}

// ── 5. Microsoft Fabric capacity (workspace created post-deploy via REST API) ───
resource fabricCapacity 'Microsoft.Fabric/capacities@2023-11-01' = if (deployFabric) {
  name: fabricCapacityName
  location: fabricLocation
  tags: tags
  sku: {
    name: fabricSkuName
    tier: 'Fabric'
  }
  properties: {
    administration: {
      members: empty(fabricAdminMembers) ? [ownerEmail] : fabricAdminMembers
    }
  }
}

// ── 4. Azure Databricks workspace (no cluster — DBU only when clusters run) ─────
resource databricksWorkspace 'Microsoft.Databricks/workspaces@2024-05-01' = {
  name: databricksName
  location: databricksLocation
  tags: tags
  sku: {
    name: 'premium'
  }
  properties: {
    managedResourceGroupId: subscriptionResourceId('Microsoft.Resources/resourceGroups', managedRgName)
    parameters: {
      enableNoPublicIp: {
        value: false
      }
    }
    publicNetworkAccess: 'Enabled'
  }
}

output dataFactoryName string = dataFactory.name
output synapseWorkspaceName string = deploySynapse ? synapseWorkspace.name : 'skipped-sql-provisioning-blocked'
output synapseSqlEndpoint string = deploySynapse ? synapseWorkspace.properties.connectivityEndpoints.sql : ''
output purviewAccountName string = deployPurview ? purviewAccount.name : 'skipped'
output purviewPortalUrl string = deployPurview ? 'https://web.purview.azure.com/resource/${purviewAccount.id}/overview' : ''
output fabricCapacityName string = deployFabric ? fabricCapacity.name : 'skipped'
output fabricCapacityId string = deployFabric ? fabricCapacity.id : ''
output databricksWorkspaceName string = databricksWorkspace.name
output databricksManagedResourceGroup string = managedRgName
