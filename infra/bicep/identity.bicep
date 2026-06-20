// Layer 6 — user-assigned managed identity. See README.md section 3,
// "User-assigned vs system-assigned managed identity" decision: chosen
// over system-assigned specifically because this project's Function and
// ADF resources get redeployed repeatedly during active development, and
// a system-assigned identity is deleted along with its resource (breaking
// any role assignment that referenced it). A user-assigned identity is
// provisioned once here, attached to multiple resources across this repo
// (Function App, ADF, Synapse — see function.bicep, synapse.bicep,
// cosmosdb.bicep, all of which take this module's output as a parameter),
// and its access stays stable across redeploys of any one of those.
//
// ONE identity shared across all of this project's compute resources,
// not one per resource. This is a deliberate simplification appropriate
// for a single-project, single-environment-at-a-time deployment — a
// larger system with genuinely different blast-radius requirements per
// component might reasonably want per-resource identities instead, but
// that wasn't part of any documented decision here, and adding that
// complexity without a reason to would be over-engineering for this
// project's actual scope.

@description('Azure region for the managed identity')
param location string = resourceGroup().location

@description('Name of the user-assigned managed identity')
param identityName string = 'uami-iot-pipeline'

@description('Environment tag (dev/prod)')
param environment string = 'dev'

resource userAssignedIdentity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' = {
  name: identityName
  location: location
  tags: {
    environment: environment
    layer: 'security-and-observability'
  }
}

// Outputs consumed by every other module that needs this identity:
// keyvault.bicep (role assignment target), function.bicep (Function App
// identity binding), cosmosdb.bicep (account identity), synapse.bicep
// (workspace identity). Exposing both the resource ID (needed when a
// module references this as `identity.userAssignedIdentities[...]`) and
// the principal ID (needed for role assignments, which bind to a
// principal ID, not a resource ID) since downstream modules need
// different shapes of the same identity depending on what they're doing
// with it.
output identityResourceId string = userAssignedIdentity.id
output identityPrincipalId string = userAssignedIdentity.properties.principalId
output identityClientId string = userAssignedIdentity.properties.clientId
