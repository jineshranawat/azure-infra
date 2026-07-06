// Shared student estate — eastus only (explicit exception: training subscription quota).
// All learners use LEARNER=shared and AZURE_SUBSCRIPTION_ID for the class subscription.
// Deploy: provision-shared.cmd  (or scripts/provision_shared.py)
targetScope = 'resourceGroup'

@description('eastus — shared training subscription (documented region exception).')
@allowed(['eastus'])
param location string = 'eastus'

@description('Shared identifier for globally-unique resource names (default: shared).')
@minLength(2)
@maxLength(10)
param learner string = 'shared'

@description('Owner email for mandatory tags and budget alert notifications.')
param ownerEmail string

@description('Object ID of the signed-in user or service principal — data-plane RBAC.')
param principalObjectId string

@description('Principal type for data-plane RBAC role assignments.')
@allowed(['User', 'ServicePrincipal'])
param principalType string = 'User'

@description('First day of the current month (YYYY-MM-01) — budget timePeriod anchor.')
param budgetStartDate string

@description('Deploy consumption budget (unsupported on Azure Sponsorship subscriptions).')
param deployBudget bool = false

var tags = {
  env: 'training'
  owner: ownerEmail
  costcentre: 'boe-data-enablement'
  'data-class': 'training-synthetic'
  course: 'azure-etl-boe'
  class: 'shared-eastus'
  'auto-teardown': 'manual'
  estate: 'shared'
}

var nameHash = substring(uniqueString(resourceGroup().id, learner, 'shared-eastus'), 0, 6)
var storageAccountName = toLower('st${learner}${nameHash}')
var keyVaultName = 'kv-${learner}-${nameHash}'
var adfName = 'adf-${learner}-${nameHash}'
var databricksName = 'dbw-${learner}-${nameHash}'
var managedRgName = 'rg-${learner}-dbw-${nameHash}'

var kvSecretsOfficerRoleId = 'b86a8fe4-44ce-4948-aee5-eccb2c155cd7'
var storageBlobDataContributorRoleId = 'ba92f5b4-2d11-453d-a403-e96b0029c9fe'

// ── Budget guardrail (optional — Sponsorship subscriptions often block Cost Management budgets) ──
resource budget 'Microsoft.Consumption/budgets@2023-11-01' = if (deployBudget) {
  name: 'budget-${learner}-shared'
  properties: {
    category: 'Cost'
    amount: 50
    timeGrain: 'Monthly'
    timePeriod: {
      startDate: budgetStartDate
      endDate: '2030-12-31'
    }
    filter: {
      dimensions: {
        name: 'ResourceGroupName'
        operator: 'In'
        values: [resourceGroup().name]
      }
    }
    notifications: {
      Actual_GreaterThan_50_Pct: {
        enabled: true
        operator: 'GreaterThan'
        threshold: 50
        thresholdType: 'Actual'
        contactEmails: [ownerEmail]
      }
      Forecast_GreaterThan_90_Pct: {
        enabled: true
        operator: 'GreaterThan'
        threshold: 90
        thresholdType: 'Forecasted'
        contactEmails: [ownerEmail]
      }
    }
  }
}

// ── Key Vault (RBAC, Standard) ────────────────────────────────────────────────
resource keyVault 'Microsoft.KeyVault/vaults@2023-07-01' = {
  name: keyVaultName
  location: location
  tags: tags
  properties: {
    tenantId: subscription().tenantId
    sku: {
      family: 'A'
      name: 'standard'
    }
    enableRbacAuthorization: true
    enabledForDeployment: false
    enabledForDiskEncryption: false
    enabledForTemplateDeployment: false
    enableSoftDelete: true
    softDeleteRetentionInDays: 7
    publicNetworkAccess: 'Enabled'
  }
}

// ── ADLS Gen2 storage (medallion containers) ──────────────────────────────────
resource storageAccount 'Microsoft.Storage/storageAccounts@2023-05-01' = {
  name: storageAccountName
  location: location
  tags: tags
  sku: {
    name: 'Standard_LRS'
  }
  kind: 'StorageV2'
  properties: {
    accessTier: 'Hot'
    isHnsEnabled: true
    allowBlobPublicAccess: false
    minimumTlsVersion: 'TLS1_2'
    supportsHttpsTrafficOnly: true
  }
}

resource blobService 'Microsoft.Storage/storageAccounts/blobServices@2023-05-01' = {
  parent: storageAccount
  name: 'default'
  properties: {
    deleteRetentionPolicy: {
      enabled: true
      days: 7
    }
    containerDeleteRetentionPolicy: {
      enabled: true
      days: 7
    }
  }
}

var containerNames = ['bronze', 'silver', 'gold', 'audit']

resource containers 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = [
  for name in containerNames: {
    parent: blobService
    name: name
    properties: {
      publicAccess: 'None'
    }
  }
]

resource lifecyclePolicy 'Microsoft.Storage/storageAccounts/managementPolicies@2023-05-01' = {
  parent: storageAccount
  name: 'default'
  properties: {
    policy: {
      rules: [
        {
          enabled: true
          name: 'tier-block-blobs-cool-30d'
          type: 'Lifecycle'
          definition: {
            actions: {
              baseBlob: {
                tierToCool: {
                  daysAfterModificationGreaterThan: 30
                }
              }
            }
            filters: {
              blobTypes: ['blockBlob']
            }
          }
        }
        {
          enabled: true
          name: 'delete-block-blobs-7d'
          type: 'Lifecycle'
          definition: {
            actions: {
              baseBlob: {
                delete: {
                  daysAfterModificationGreaterThan: 7
                }
              }
            }
            filters: {
              blobTypes: ['blockBlob']
            }
          }
        }
      ]
    }
  }
}

// ── Data-plane RBAC for deployer ──────────────────────────────────────────────
resource kvRoleAssignment 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(keyVault.id, principalObjectId, kvSecretsOfficerRoleId)
  scope: keyVault
  properties: {
    roleDefinitionId: concat('/subscriptions/', subscription().subscriptionId, '/providers/Microsoft.Authorization/roleDefinitions/', kvSecretsOfficerRoleId)
    principalId: principalObjectId
    principalType: principalType
  }
}

resource storageRoleAssignment 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(storageAccount.id, principalObjectId, storageBlobDataContributorRoleId)
  scope: storageAccount
  properties: {
    roleDefinitionId: concat('/subscriptions/', subscription().subscriptionId, '/providers/Microsoft.Authorization/roleDefinitions/', storageBlobDataContributorRoleId)
    principalId: principalObjectId
    principalType: principalType
  }
}

// ── Azure Data Factory ────────────────────────────────────────────────────────
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

// ── Databricks workspace (eastus — no access connector; use Single-user clusters in labs) ──
resource databricksWorkspace 'Microsoft.Databricks/workspaces@2024-05-01' = {
  name: databricksName
  location: location
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

output storageAccountName string = storageAccount.name
output keyVaultName string = keyVault.name
output dfsEndpoint string = storageAccount.properties.primaryEndpoints.dfs
output dataFactoryName string = dataFactory.name
output databricksWorkspaceName string = databricksWorkspace.name
output databricksManagedResourceGroup string = managedRgName
output location string = location
output resourceGroupName string = resourceGroup().name
