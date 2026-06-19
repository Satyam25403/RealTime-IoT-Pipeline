// Layer 4 — Cosmos DB hot path serving. See README.md section 3
// ("Layer 4 — Hot Path Serving") for the design decisions this implements:
// serverless capacity mode (cost decision — this workload is a handful of
// writes/hour, not a sustained load that would justify provisioned RU/s),
// city_id as the partition key (query pattern is always "alerts for city
// X" or "recent alerts," and city_id has enough cardinality across the
// 12-city fleet to avoid a hot partition).
//
// IMPORTANT: serverless accounts (capabilities: [{name: 'EnableServerless'}])
// must NOT specify a `throughput` property on the database or container --
// that combination is invalid and the deployment will fail. This is the
// opposite of a provisioned-throughput account, where omitting throughput
// is the mistake. Don't "fix" this file by adding a throughput parameter
// without also removing the EnableServerless capability.
//
// Document id and partition key path here (/city_id) must match exactly
// what stream_processing/asa_queries/anomaly_detection.asaql actually
// emits -- ASA's Cosmos DB output partitions writes based on the query's
// own PARTITION BY clause, and per Microsoft's docs Stream Analytics only
// supports unlimited containers with a partition key at the TOP LEVEL
// (e.g. /city_id is supported; a nested path is not) -- see that query's
// header comment for the full reasoning.

@description('Azure region for the Cosmos DB account')
param location string = resourceGroup().location

@description('Globally unique Cosmos DB account name')
param cosmosAccountName string

@description('Name of the user-assigned managed identity ASA and any reader apps use to access this account -- see infra/bicep/identity.bicep')
param userAssignedIdentityResourceId string

@description('Environment tag (dev/prod) -- propagated from infra/parameters/*.parameters.json')
param environment string = 'dev'

var databaseName = 'weather_lakehouse'
var containerName = 'anomaly_alerts'

resource cosmosAccount 'Microsoft.DocumentDB/databaseAccounts@2023-11-15' = {
  name: cosmosAccountName
  location: location
  kind: 'GlobalDocumentDB'
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${userAssignedIdentityResourceId}': {}
    }
  }
  properties: {
    databaseAccountOfferType: 'Standard'
    locations: [
      {
        locationName: location
        failoverPriority: 0
        isZoneRedundant: false
      }
    ]
    capabilities: [
      {
        name: 'EnableServerless' // see file header -- no throughput params anywhere below because of this
      }
    ]
    consistencyPolicy: {
      // Session consistency: the README doesn't call out a specific
      // consistency-level decision, and Session is the right unopinionated
      // default for a single-writer (ASA), single-region account like this
      // one -- strong consistency adds latency with no benefit here since
      // there's no multi-region write conflict to resolve, and eventual
      // consistency would risk Power BI's Change Feed dashboard showing a
      // stale alert list immediately after a write.
      defaultConsistencyLevel: 'Session'
    }
    // Required for the Power BI live alert dashboard (see README Layer 4 +
    // powerbi/README.md): Power BI does NOT connect to Cosmos DB's
    // transactional Change Feed directly -- there's no native connector
    // for that. The supported "live dashboard, no ETL" path is Azure
    // Synapse Link + DirectQuery, which requires analytical storage
    // enabled at the account level (this property) AND on the container
    // (see anomalyAlertsContainer below).
    //
    // ONE-WAY DECISION, flagged deliberately: per Microsoft's own docs,
    // "after enabling Azure Synapse Link for an account, you can't disable
    // it." This is a real deployment consideration, not just a feature
    // flag -- if cost or complexity concerns ever make Synapse Link
    // undesirable, that decision has to be made BEFORE first deploy, not
    // after.
    enableAnalyticalStorage: true
    analyticalStorageConfiguration: {
      schemaType: 'WellDefined' // default for API for NoSQL; FullFidelity only matters for Mongo API accounts
    }
    // No virtual network / private endpoint config here yet -- README's
    // Layer 6 decision calls for private endpoints on Cosmos DB; that's a
    // separate networking concern layered on top of this account once
    // infra/bicep/main.bicep wires a VNet through, not duplicated here.
  }
  tags: {
    environment: environment
    layer: 'hot-path-serving'
  }
}

resource database 'Microsoft.DocumentDB/databaseAccounts/sqlDatabases@2023-11-15' = {
  parent: cosmosAccount
  name: databaseName
  properties: {
    resource: {
      id: databaseName
    }
    // NOTE: no `options.throughput` here -- serverless accounts compute
    // cost per-request, not from a provisioned RU/s value set at the
    // database or container level. Setting this would conflict with
    // EnableServerless above.
  }
}

resource anomalyAlertsContainer 'Microsoft.DocumentDB/databaseAccounts/sqlDatabases/containers@2023-11-15' = {
  parent: database
  name: containerName
  properties: {
    resource: {
      id: containerName
      partitionKey: {
        paths: [
          '/city_id' // must match anomaly_detection.asaql's PARTITION BY city_id exactly
        ]
        kind: 'Hash'
        version: 2
      }
      // Enables Synapse Link sync for THIS container specifically --
      // enabling it at the account level above is necessary but not
      // sufficient (per Microsoft's docs: "enabling Synapse Link doesn't
      // start synchronization of operational data -- you must also create
      // or update a container with support for an analytical store").
      // -1 = infinite retention in the analytical store. This is
      // independent of the transactional store's own TTL (left unset
      // below, same as before) -- analytical and transactional retention
      // are two separate knobs.
      analyticalStorageTtl: -1
      // No defaultTtl set on the transactional store -- alerts are read by
      // both the Synapse Link/Power BI dashboard and ad hoc debugging
      // queries; expiring them automatically wasn't part of any documented
      // decision, so leaving this unset rather than guessing a retention
      // window. Revisit if Cosmos storage cost becomes a real concern --
      // flag this as an open question rather than silently picking a number.
      indexingPolicy: {
        indexingMode: 'consistent'
        automatic: true
        includedPaths: [
          { path: '/*' }
        ]
        excludedPaths: [
          // Exclude the large raw fields from indexing to reduce RU cost
          // on writes -- this container only ever needs to be queried by
          // city_id (the partition key, always indexed) and detected_at
          // (range queries for "recent alerts"), never by
          // window_max_pm2_5 or the other numeric fields individually.
          { path: '/window_max_pm2_5/?' }
          { path: '/"_etag"/?' }
        ]
      }
    }
    // No `options.throughput` here either -- same serverless reasoning as
    // the database resource above.
  }
}

output cosmosAccountEndpoint string = cosmosAccount.properties.documentEndpoint
output cosmosDatabaseName string = databaseName
output cosmosContainerName string = containerName
