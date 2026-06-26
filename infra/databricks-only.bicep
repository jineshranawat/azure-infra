// Fast path — Databricks workspace only in eastus2 (no ADF/Purview/Fabric).
// Same naming as platform-services.bicep so incremental full deploy stays idempotent.
targetScope = 'resourceGroup'

@minLength(2)
@maxLength(10)
param learner string

param ownerEmail string

@description('Databricks workspace region (eastus2 — training subscription quota).')
param databricksLocation string = 'eastus2'

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
var databricksName = 'dbw-${learner}-${nameHash}'
var managedRgName = 'rg-${learner}-dbw-${nameHash}'

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

output databricksWorkspaceName string = databricksWorkspace.name
output databricksManagedResourceGroup string = managedRgName
output databricksLocation string = databricksLocation
