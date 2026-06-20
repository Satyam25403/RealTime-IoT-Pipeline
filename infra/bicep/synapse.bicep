// Layer 5 — Synapse workspace, SERVERLESS SQL ONLY. See README.md section
// 3 ("OPENROWSET vs PolyBase, serverless vs dedicated pools") for why this
// workspace deliberately has no dedicated SQL pool provisioned anywhere in
// this file -- dedicated pools bill continuously per DWU regardless of
// query activity, which conflicts with this project's cost-conscious goal
// for an intermittent BI workload. The serverless SQL endpoint is created
// automatically as part of ANY Synapse workspace at no extra cost -- there
// is no separate "create serverless pool" resource to provision; simply
// not adding a Microsoft.Synapse/workspaces/sqlPools resource here IS the
// decision.
//
// AUTHENTICATION: azureADOnlyAuthentication: true, NOT a SQL admin
// login/password. Per Microsoft's own docs, "Microsoft Entra pass-through
// is the default behavior when you create a workspace" and managed
// identity is a supported authorization type for serverless SQL pool
// storage access -- this fits README Layer 6's "managed identities
// everywhere" decision far better than the password-based pattern shown
// in most generic Synapse Bicep examples, which this file deliberately
// does NOT copy.
//
// DEPENDENCY ON storage.bicep: a Synapse workspace requires
// `defaultDataLakeStorage` pointing at a real ADLS Gen2 filesystem at
// creation time -- this is not optional. storage.bicep is still a stub as
// of this file being written (see README implementation status table), so
// the storage account details are taken as PARAMETERS here, to be wired
// from storage.bicep's own outputs once that module exists, rather than
// this file guessing at a storage account name/URL that doesn't exist yet.
// main.bicep is where that wiring will actually happen
// (storageModule.outputs.xxx -> this module's params).

@description('Azure region for the Synapse workspace')
param location string = resourceGroup().location

@description('Globally unique Synapse workspace name')
param synapseWorkspaceName string

@description('ADLS Gen2 dfs endpoint URL of the storage account this workspace uses as its default data lake -- comes from storage.bicep once implemented, e.g. storageModule.outputs.primaryDfsEndpoint')
param defaultDataLakeStorageAccountUrl string

@description('Filesystem (container) name within that storage account for Synapse workspace metadata -- NOT the gold/silver/bronze containers themselves, which are queried via the external data source in cold_path/synapse_sql, not this property')
param defaultDataLakeFilesystemName string

@description('Resource ID of the user-assigned managed identity this workspace uses -- see infra/bicep/identity.bicep')
param userAssignedIdentityResourceId string

@description('Environment tag (dev/prod)')
param environment string = 'dev'

resource synapseWorkspace 'Microsoft.Synapse/workspaces@2021-06-01' = {
  name: synapseWorkspaceName
  location: location
  identity: {
    type: 'SystemAssigned,UserAssigned' // SystemAssigned required by Synapse itself for internal operations;
                                          // UserAssigned is what this project's other resources (Function, ADF)
                                          // share for consistent, stable cross-resource access per README Layer 6
    userAssignedIdentities: {
      '${userAssignedIdentityResourceId}': {}
    }
  }
  properties: {
    defaultDataLakeStorage: {
      accountUrl: defaultDataLakeStorageAccountUrl
      filesystem: defaultDataLakeFilesystemName
    }
    // No SQL admin login/password -- see file header. Entra-only auth.
    azureADOnlyAuthentication: true
    managedVirtualNetwork: 'default' // keeps Synapse-managed compute (serverless SQL pool execution)
                                       // inside a managed VNet, consistent with README Layer 6's
                                       // "private endpoints on storage" decision -- a workspace outside
                                       // a managed VNet can't use managed private endpoints to reach
                                       // storage privately
    publicNetworkAccess: 'Disabled'   // see Layer 6 -- private endpoints, not public access, for every
                                       // resource in this project; this matches that posture for Synapse's
                                       // own workspace endpoint (not the same thing as storage access,
                                       // which is governed separately by the managed private endpoint
                                       // implied by managedVirtualNetwork above)
  }
  tags: {
    environment: environment
    layer: 'cold-path-serving'
  }
}

// No firewall rules resource here on purpose -- the common "allow all IPs"
// firewall rule seen in many tutorial Bicep examples (0.0.0.0-255.255.255.255)
// is exactly what publicNetworkAccess: 'Disabled' above is meant to avoid.
// Access to the serverless SQL endpoint and Synapse Studio happens through
// private endpoints / managed VNet only.

output synapseWorkspaceName string = synapseWorkspace.name
output serverlessSqlEndpoint string = synapseWorkspace.properties.connectivityEndpoints.sqlOnDemand
