// Class-1 landing zone — resource-group-scoped Bicep template.
// Hour 1: residency + governance tags. Hour 2: guardrail-first budgeting.
// Hour 3: identity (Key Vault RBAC). Hour 4: medallion lake + lifecycle.
// Hour 5: least-privilege data-plane RBAC (Owner ≠ Blob Data Contributor).
targetScope = 'resourceGroup'

@description('UK-only region — satisfies BoE data residency for training estates.')
@allowed([
  'uksouth'
  'ukwest'
])
param location string = 'uksouth'

@description('Short learner identifier used in globally-unique resource names.')
@minLength(2)
@maxLength(10)
param learner string

@description('Owner email for mandatory tags and budget alert notifications.')
param ownerEmail string

@description('Object ID of the signed-in user — data-plane RBAC requires explicit assignment.')
param principalObjectId string

@description('First day of the current month (YYYY-MM-01) — budget timePeriod anchor.')
param budgetStartDate string

// ── Mandatory governance tags (applied to every resource in this deployment) ──
var tags = {
  env: 'training'
  owner: ownerEmail
  costcentre: 'boe-data-enablement'
  'data-class': 'training-synthetic'
  course: 'azure-etl-boe'
  class: 'class-1'
  'auto-teardown': 'nightly'
}

// uniqueString keeps names deterministic per RG while staying globally unique.
var nameHash = substring(uniqueString(resourceGroup().id, learner), 0, 6)
var storageAccountName = toLower('st${learner}${nameHash}')
var keyVaultName = 'kv-${learner}-${nameHash}'

// ── BUILD ORDER step 2: budget guardrail BEFORE spend-capable resources ──────
// Budgets are free; £1 cap with early alerts teaches cost governance (Hour 2).
resource budget 'Microsoft.Consumption/budgets@2023-11-01' = {
  name: 'budget-${learner}-class1'
  properties: {
    category: 'Cost'
    amount: 1
    timeGrain: 'Monthly'
    timePeriod: {
      startDate: budgetStartDate
      endDate: '2030-12-31'
    }
    filter: {
      dimensions: {
        name: 'ResourceGroupName'
        operator: 'In'
        values: [
          resourceGroup().name
        ]
      }
    }
    notifications: {
      Actual_GreaterThan_50_Pct: {
        enabled: true
        operator: 'GreaterThan'
        threshold: 50
        thresholdType: 'Actual'
        contactEmails: [
          ownerEmail
        ]
      }
      Forecast_GreaterThan_90_Pct: {
        enabled: true
        operator: 'GreaterThan'
        threshold: 90
        thresholdType: 'Forecasted'
        contactEmails: [
          ownerEmail
        ]
      }
    }
  }
}

// ── BUILD ORDER step 3: identity & secrets vault (RBAC mode, Standard = £0) ──
resource keyVault 'Microsoft.KeyVault/vaults@2023-07-01' = {
  name: keyVaultName
  location: location
  tags: tags
  properties: {
    tenantId: subscription().tenantId
    sku: {
      // Standard tier has no fixed monthly fee — Premium would violate £0 constraint.
      family: 'A'
      name: 'standard'
    }
    // RBAC mode: assignments via role definitions, not legacy access policies (Hour 3).
    enableRbacAuthorization: true
    enabledForDeployment: false
    enabledForDiskEncryption: false
    enabledForTemplateDeployment: false
    enableSoftDelete: true
    softDeleteRetentionInDays: 7
    publicNetworkAccess: 'Enabled'
  }
}

// ── BUILD ORDER step 4: medallion lake — StorageV2 + ADLS Gen2 (HNS) ────────
resource storageAccount 'Microsoft.Storage/storageAccounts@2023-05-01' = {
  name: storageAccountName
  location: location
  tags: tags
  sku: {
    // Standard_LRS in UK region — no GRS/ZRS/Premium fixed charges (Hour 4).
    name: 'Standard_LRS'
  }
  kind: 'StorageV2'
  properties: {
    accessTier: 'Hot'
    // Hierarchical namespace enables bronze/silver/gold as data-lake filesystems.
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
    // Soft-delete + versioning: recoverability without paid backup SKUs (Hour 4).
    deleteRetentionPolicy: {
      enabled: true
      days: 7
    }
    containerDeleteRetentionPolicy: {
      enabled: true
      days: 7
    }
    // Blob versioning is incompatible with HNS (ADLS Gen2) — soft-delete only.
  }
}

// Medallion containers — idempotent child resources, safe to redeploy.
var containerNames = [
  'bronze'
  'silver'
  'gold'
  'audit'
]

resource containers 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = [for name in containerNames: {
  parent: blobService
  name: name
  properties: {
    publicAccess: 'None'
  }
}]

// Lifecycle: training data self-expires (delete @ 7d); Cool tier rule teaches tiering (Hour 4).
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
              blobTypes: [
                'blockBlob'
              ]
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
              blobTypes: [
                'blockBlob'
              ]
            }
          }
        }
      ]
    }
  }
}

// ── BUILD ORDER step 5: data-plane RBAC — control-plane Owner is insufficient ─
var kvSecretsOfficerRoleId = 'b86a8fe4-44ce-4948-aee5-586e75ac9c75'
var storageBlobDataContributorRoleId = 'ba92f5b4-2d11-453d-a403-e96b0029c9fe'

resource kvRoleAssignment 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(keyVault.id, principalObjectId, kvSecretsOfficerRoleId)
  scope: keyVault
  properties: {
    roleDefinitionId: concat('/subscriptions/', subscription().subscriptionId, '/providers/Microsoft.Authorization/roleDefinitions/', kvSecretsOfficerRoleId)
    principalId: principalObjectId
    principalType: 'User'
  }
}

resource storageRoleAssignment 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(storageAccount.id, principalObjectId, storageBlobDataContributorRoleId)
  scope: storageAccount
  properties: {
    roleDefinitionId: concat('/subscriptions/', subscription().subscriptionId, '/providers/Microsoft.Authorization/roleDefinitions/', storageBlobDataContributorRoleId)
    principalId: principalObjectId
    principalType: 'User'
  }
}

// ── Outputs for learners / verify_cost.py cross-check ─────────────────────────
output storageAccountName string = storageAccount.name
output keyVaultName string = keyVault.name
output dfsEndpoint string = storageAccount.properties.primaryEndpoints.dfs
output location string = location
output tags object = tags
output resourceGroupName string = resourceGroup().name
