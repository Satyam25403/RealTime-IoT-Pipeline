// Layer 6 — Key Vault, storing the OpenWeatherMap API key. See README.md
// section 3 ("Layer 1 — Ingestion": "Store the API key in Key Vault,
// accessed via managed identity") and ingestion/function_app/shared/
// key_vault.py, which is the actual consumer of what this module
// provisions -- the secret NAME below ('owm-api-key') and the env var
// this module's output should be wired to (KEY_VAULT_URL, set as a
// Function App setting in function.bicep) must match that file exactly,
// or Layer 1's Azure deployment path silently breaks while the local-dev
// env-var path keeps working, masking the problem.
//
// ACCESS MODEL: Azure RBAC, not the legacy access-policy model.
// enableRbacAuthorization: true is set explicitly below. This matters
// more than it might look like a one-line detail: as of API version
// 2026-02-01, Azure RBAC became the DEFAULT access control model for
// newly created key vaults -- but this module sets it explicitly anyway,
// rather than relying on the default, so the decision is visible in code
// and doesn't silently change behavior if a future API version's default
// changes again. RBAC was also already the right fit here independent of
// the new default: it's the same role-assignment pattern used for Cosmos
// DB and Synapse elsewhere in this repo (see cosmosdb.bicep,
// synapse.bicep), so this keeps the access-control story consistent
// across every resource rather than mixing two different models.

@description('Azure region for the Key Vault')
param location string = resourceGroup().location

@description('Globally unique Key Vault name (3-24 chars, alphanumeric and hyphens)')
param keyVaultName string

@description('Principal ID of the user-assigned managed identity that needs to read secrets from this vault -- see infra/bicep/identity.bicep output identityPrincipalId')
param readerPrincipalId string

@description('The OpenWeatherMap API key value to store. Marked @secure so it never appears in deployment logs/outputs -- pass via a secure parameter file or pipeline secret, never committed to infra/parameters/*.json in plaintext.')
@secure()
param owmApiKeyValue string

@description('Environment tag (dev/prod)')
param environment string = 'dev'

var keyVaultSecretsUserRoleId = '4633458b-17de-408a-b874-0445c86b69e6' // built-in role, stable GUID across all tenants --
                                                                          // verified against Microsoft's own role definitions
                                                                          // listing before hardcoding; data actions are limited
                                                                          // to getSecret + readMetadata, i.e. genuinely read-only,
                                                                          // which matches key_vault.py's only usage (get_secret())

resource keyVault 'Microsoft.KeyVault/vaults@2024-11-01' = {
  name: keyVaultName
  location: location
  properties: {
    sku: {
      family: 'A'
      name: 'standard'
    }
    tenantId: subscription().tenantId
    enableRbacAuthorization: true // see file header -- explicit, not relying on the API version's default
    enableSoftDelete: true        // default is already true on current API versions, but stated explicitly
                                    // rather than assumed -- accidental deletion of the OWM key shouldn't be
                                    // unrecoverable
    softDeleteRetentionInDays: 7  // minimum allowed; this project has no compliance requirement calling for
                                    // longer retention, and shorter recovery windows are fine for a single
                                    // non-sensitive API key (not a customer secret or cryptographic key)
    enablePurgeProtection: false  // deliberately NOT enabled -- purge protection prevents permanent deletion
                                    // even by an Owner, for the soft-delete retention period. For a project
                                    // that may need a full teardown (see infra/scripts/teardown.sh) without
                                    // waiting out a retention window, this would actively get in the way with
                                    // no compensating benefit for a single non-sensitive secret. A vault
                                    // storing real production secrets would likely want this enabled --
                                    // documented as a context-specific call, not a universal default.
    publicNetworkAccess: 'Disabled' // README Layer 6 -- private endpoints, not public access, everywhere
    networkAcls: {
      defaultAction: 'Deny'
      bypass: 'AzureServices'
    }
  }
  tags: {
    environment: environment
    layer: 'security-and-observability'
  }
}

resource owmApiKeySecret 'Microsoft.KeyVault/vaults/secrets@2024-11-01' = {
  parent: keyVault
  name: 'owm-api-key' // MUST match shared/key_vault.py's SECRET_NAME constant exactly
  properties: {
    value: owmApiKeyValue
  }
}

resource secretsUserRoleAssignment 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(keyVault.id, readerPrincipalId, keyVaultSecretsUserRoleId)
  scope: keyVault
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', keyVaultSecretsUserRoleId)
    principalId: readerPrincipalId
    principalType: 'ServicePrincipal' // managed identities are represented as service principals for RBAC purposes
  }
}

// Consumed by function.bicep (as a Function App setting) and by anyone
// running the Function locally needs the equivalent value documented in
// README.md section 5a / local.settings.json.example instead.
output keyVaultUri string = keyVault.properties.vaultUri
