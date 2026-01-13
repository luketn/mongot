# Description

The metadata service stores internal metadata on the clusters mongod instance in the
internal '__mdb_internal_search' database. This is made up of 3 collections:

## __mdb_internal_search.indexCatalog

Stores the authoritative index definitions as requested by the customer.

```
{
  _id: {
    // UUID of the collection the index is created against
    collectionUUID: String,
    // Name of the index, uniquely identifies the index
    name: String
  },
  // Existing ObjectID-based index id
  indexId: ObjectId,
  // Index catalog schema version
  schemaVersion: Number,
  // Embedded document containing the actual index definition
  definition: IndexDefinition
}
```

## __mdb_internal_search.indexStats

Stores up-to-date stats on each index per server.

```
{
  _id: {
    // ObjectID to uniquely identify each server
    serverId: <String>,
    // Existing ObjectID-based index id
    indexId: ObjectID
  },
  // search or vector
  type: <String>,
  mainIndex: DetailedIndexStats,
  stagedIndex: DetailedIndexStats
}

// Closely matches the IndexDetailedStatus
DetailedIndexStats: {
  indexStatus: IndexStatus,
  definition: IndexDefinition,
  synonymMappingStatus: SynonymStatus,
  synonymMappingDetail: [
    name: <String>,
    status: SynonymDetailedStatus
  ],
}
```

### __mdb_internal_search.serverState

Stores active mongot servers in the cluster

```
{
    // Internal ID to identify each server
   _id: <ObjectID>,
   // Provided through mongot community config
   serverName: <String>, 
   // Servers heartbeat to indicate livelyness
   lastHeartbeatTs: <Date>
}
```