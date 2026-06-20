// Layer 6 — centralized Log Analytics workspace. See README.md section 3
// ("Centralize logs in a Log Analytics workspace"). This module provisions
// ONLY the workspace itself -- the actual diagnostic settings that send
// each resource's logs INTO this workspace are extension resources scoped
// to THOSE resources (Microsoft.Insights/diagnosticSettings), not to this
// one, and per-resource log category names vary by resource type, so they
// belong in each resource's own Bicep module, not centralized here as one
// big guess. See the bottom of this file for the wiring template each
// resource's module should adopt once implemented.
//
// HONEST STATUS: as of this file being written, eventhub.bicep,
// streamanalytics.bicep, and an ADF/Data Factory module don't exist yet
// (all still stubs -- see README implementation status table). The three
// KQL queries in docs/kql/ that this workspace is meant to serve
// (eventhub_consumer_lag.kql, asa_su_utilization.kql,
// adf_pipeline_failures.kql) are written against the table/column shapes
// those diagnostic settings WOULD produce once wired up -- this workspace
// existing is a precondition for those queries to return real data, not a
// guarantee they already do.

@description('Azure region for the Log Analytics workspace')
param location string = resourceGroup().location

@description('Log Analytics workspace name')
param workspaceName string = 'log-iot-pipeline'

@description('Retention period in days. 30 is the free-tier-friendly default and is enough to debug recent failures (the KQL queries in docs/kql/ all look at recent activity, not long-horizon trend analysis) -- not chosen for any compliance reason, since none was documented for this project.')
param retentionInDays int = 30

@description('Environment tag (dev/prod)')
param environment string = 'dev'

resource logAnalyticsWorkspace 'Microsoft.OperationalInsights/workspaces@2023-09-01' = {
  name: workspaceName
  location: location
  properties: {
    sku: {
      name: 'PerGB2018' // pay-per-GB ingested, no fixed capacity commitment -- consistent with this
                          // project's moderate-cost posture (README's overall cost-consciousness goal),
                          // appropriate for a workspace receiving a handful of resources' diagnostic
                          // logs at low volume, not a high-ingestion enterprise workload
    }
    retentionInDays: retentionInDays
    publicNetworkAccessForIngestion: 'Enabled'  // diagnostic settings from Azure resources need to reach
                                                   // this workspace; this is the ingestion path, separate
                                                   // from query access below
    publicNetworkAccessForQuery: 'Disabled'      // README Layer 6 -- query access (e.g. running the KQL
                                                   // files in docs/kql/) should go through private
                                                   // endpoints / the Azure portal's own authenticated path,
                                                   // not be open to the public internet
  }
  tags: {
    environment: environment
    layer: 'security-and-observability'
  }
}

output logAnalyticsWorkspaceId string = logAnalyticsWorkspace.id
output logAnalyticsWorkspaceName string = logAnalyticsWorkspace.name
output logAnalyticsCustomerId string = logAnalyticsWorkspace.properties.customerId

// =============================================================================
// WIRING TEMPLATE -- not deployed by this file, copied into each resource's
// OWN module once implemented. Shown here as a documented reference so the
// "centralize logs" decision has a concrete, checkable shape rather than
// staying an abstract intention.
// =============================================================================
//
// Example for Event Hub (would live in eventhub.bicep once implemented):
//
//   resource eventHubDiagnostics 'Microsoft.Insights/diagnosticSettings@2021-05-01-preview' = {
//     name: 'diag-eventhub-to-loganalytics'
//     scope: eventHubNamespace   // the actual Event Hub namespace resource in that file
//     properties: {
//       workspaceId: logAnalyticsWorkspaceId   // this module's output, passed in as a param
//       logs: [
//         { category: 'ArchiveLogs', enabled: true }
//         { category: 'OperationalLogs', enabled: true }
//         { category: 'AutoScaleLogs', enabled: true }
//         { category: 'KafkaCoordinatorLogs', enabled: true }
//         { category: 'EventHubVNetConnectionEvent', enabled: true }
//       ]
//       metrics: [
//         { category: 'AllMetrics', enabled: true }
//       ]
//     }
//   }
//
//   This is what eventhub_consumer_lag.kql's "AzureDiagnostics | where
//   ResourceProvider == 'MICROSOFT.EVENTHUB'" query actually depends on --
//   without this diagnostic setting wired up, that KQL query runs
//   successfully but returns zero rows, which looks like "no lag" but
//   actually means "no data," a distinction worth knowing before trusting
//   a quiet dashboard.
//
// Stream Analytics and ADF need the equivalent pattern in
// streamanalytics.bicep and whatever module eventually provisions the ADF
// pipeline trigger -- category names differ per resource type and should
// be looked up against that resource type's own diagnostic settings
// reference when those modules are actually implemented, not guessed at
// here in advance.
