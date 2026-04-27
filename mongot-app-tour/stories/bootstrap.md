# MongoT Bootstrap

## Scenario

MongoT starts as its own Java process from the public static void main of the MongotCommunity class.

The startup path wires configuration, logging, metrics, tracing, metadata clients, catalog services, index lifecycle services, replication services, cursor management, and the gRPC command server that mongod will use later.

MongoTCommunity handles command line arguments, which are passed to [CommunityMongotBootstrapper](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/config/provider/community/CommunityMongotBootstrapper.java), which does the main work of reading the config and setting everything up.

MongoT creates its own MongoDB Java driver client during bootstrap (not to be confused with clients which may use Java drivers to connect to MongoD from the other side). The MongoT client for MongoD is in [MongoDbMetadataClient](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/util/mongodb/MongoDbMetadataClient.java#L36), and in [MongoDbMetadataClient.createMongoClients](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/util/mongodb/MongoDbMetadataClient.java#L51), which creates internal `com.mongodb.client.MongoClient` instances for metadata and server-info reads.

## Walkthrough

1. MongoT enters [CommunityMongotBootstrapper.bootstrap](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/config/provider/community/CommunityMongotBootstrapper.java#L121).
2. It parses runtime configuration, installs logging, creates server identity, and builds the sync source configuration.
3. It creates [MongoDbMetadataClient](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/util/mongodb/MongoDbMetadataClient.java#L51), which owns MongoDB Java driver clients used by MongoT itself.
4. MongoT refreshes server information through [MongoDbMetadataClient.refreshServerInfo](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/util/mongodb/MongoDbMetadataClient.java#L101).
5. The metadata client calls [MongoDbDatabase.getServerInfo](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/util/mongodb/MongoDbDatabase.java#L43), which reads MongoDB build and replica-set information from mongod.
6. Catalog and metadata services are created in [DefaultMetadataService.create](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/catalogservice/DefaultMetadataService.java#L50).
7. The gRPC server is constructed in [GrpcStreamingServer.create](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/server/grpc/GrpcStreamingServer.java#L140). It registers command streams for search and vector search, but those streams are not used until mongod sends work.

## Driver path

MongoT uses the Java driver internally to connect to mongod:

- [InternalStreamConnectionInitializer.startHandshake](https://github.com/mongodb/mongo-java-driver/blob/main/driver-core/src/main/com/mongodb/internal/connection/InternalStreamConnectionInitializer.java#L139) sends the initial `hello`.
- [InternalStreamConnectionInitializer.createHelloCommand](https://github.com/mongodb/mongo-java-driver/blob/main/driver-core/src/main/com/mongodb/internal/connection/InternalStreamConnectionInitializer.java#L173) builds the `hello` command.
- [CommandProtocolImpl.execute](https://github.com/mongodb/mongo-java-driver/blob/main/driver-core/src/main/com/mongodb/internal/connection/CommandProtocolImpl.java#L57) sends commands over the selected MongoDB server connection.
- [InternalStreamConnection.sendAndReceive](https://github.com/mongodb/mongo-java-driver/blob/main/driver-core/src/main/com/mongodb/internal/connection/InternalStreamConnection.java#L383) writes the message to mongod and waits for the reply.

This driver path is MongoT-to-mongod metadata traffic. It is separate from the application client's driver path.

The direct evidence is [MongoClientBuilder.buildNonReplicationWithDefaults](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/util/mongodb/MongoClientBuilder.java#L122), which calls through to [MongoClientBuilder.buildNonReplicationClient](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/util/mongodb/MongoClientBuilder.java#L180), where the official driver's `MongoClients.create` constructs the client from `MongoClientSettings`.

## MongoT classes involved

- [CommunityMongotBootstrapper](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/config/provider/community/CommunityMongotBootstrapper.java#L121) builds the runtime services.
- [MongoDbMetadataClient](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/util/mongodb/MongoDbMetadataClient.java#L51) creates MongoDB clients for metadata and sync-source reads.
- [MongoDbDatabase](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/util/mongodb/MongoDbDatabase.java#L43) runs server metadata commands.
- [DefaultMetadataService](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/catalogservice/DefaultMetadataService.java#L50) creates catalog, stats, and server-state services.
- [GrpcStreamingServer](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/server/grpc/GrpcStreamingServer.java#L140) registers the gRPC command streams that mongod will call later.

## Command messages

These examples use real message field names. Long arrays or large objects may be shortened with ellipses; no replacement fields are introduced.

### MongoT -> mongod: buildInfo

```json
{
  "buildInfo": true,
  "$db": "admin"
}
```

### mongod -> MongoT: buildInfo response

```json
{
  "version": "8.2.6",
  "versionArray": [8, 2, 6, 0],
  "gitVersion": "5d25c835745d06f712320b6cdae9d50b7b43663e",
  "modules": [],
  "allocator": "tcmalloc-google",
  "javascriptEngine": "mozjs",
  "sysinfo": "deprecated",
  "openssl": {
    "running": "OpenSSL 3.0.2 15 Mar 2022",
    "compiled": "OpenSSL 3.0.2 15 Mar 2022"
  },
  "buildEnvironment": {
    "distmod": "ubuntu2204",
    "distarch": "aarch64"
  },
  "bits": 64,
  "debug": false,
  "maxBsonObjectSize": 16777216,
  "storageEngines": ["devnull", "wiredTiger"],
  "ok": 1.0
}
```

### MongoT -> mongod: replica-set configuration

```json
{
  "replSetGetConfig": 1,
  "$db": "admin"
}
```

### mongod -> MongoT: replica-set configuration response

```json
{
  "config": {
    "_id": "rs0",
    "version": 1,
    "term": 1,
    "members": [
      {
        "_id": 0,
        "host": "host.docker.internal:27017",
        "arbiterOnly": false,
        "buildIndexes": true,
        "hidden": false,
        "priority": 1,
        "tags": {},
        "secondaryDelaySecs": 0,
        "votes": 1
      }
    ],
    "protocolVersion": 1,
    "writeConcernMajorityJournalDefault": true,
    "settings": {
      "chainingAllowed": true,
      "heartbeatIntervalMillis": 2000,
      "heartbeatTimeoutSecs": 10,
      "replicaSetId": {
        "$oid": "69ec2d4750523e9d353f5639"
      }
    }
  },
  "ok": 1.0
}
```

### MongoT -> mongod: replica-set status

```json
{
  "replSetGetStatus": 1,
  "$db": "admin"
}
```

### mongod -> MongoT: replica-set status response

```json
{
  "set": "rs0",
  "date": {
    "$date": "2026-04-27T03:47:28.583Z"
  },
  "myState": 1,
  "term": 1,
  "syncSourceHost": "",
  "syncSourceId": -1,
  "heartbeatIntervalMillis": 2000,
  "majorityVoteCount": 1,
  "writeMajorityCount": 1,
  "votingMembersCount": 1,
  "writableVotingMembersCount": 1,
  "members": [
    {
      "_id": 0,
      "name": "host.docker.internal:27017",
      "health": 1.0,
      "state": 1,
      "stateStr": "PRIMARY",
      "self": true
    }
  ],
  "ok": 1.0
}
```

### MongoT -> mongod: list authoritative search index catalog entries

```json
{
  "find": "indexCatalog",
  "readConcern": {
    "level": "majority"
  },
  "filter": {},
  "$db": "__mdb_internal_search"
}
```

### mongod -> MongoT: authoritative search index catalog response

```jsonc
{
  "cursor": {
    "id": 0,
    "ns": "__mdb_internal_search.indexCatalog",
    "firstBatch": [
      {
        "_id": {
          "collectionUuid": {
            "$binary": {
              "base64": "NDqH4tFIQ/aO6fC90aniKQ==",
              "subType": "04"
            }
          },
          "indexName": "default"
        },
        "indexId": {
          "$oid": "69ec2db7a722ed3e1957e643"
        },
        "schemaVersion": 1,
        "definition": {
          "indexID": {
            "$oid": "69ec2db7a722ed3e1957e643"
          },
          "name": "default",
          "database": "clientDb",
          "lastObservedCollectionName": "image",
          "collectionUUID": {
            "$binary": {
              "base64": "NDqH4tFIQ/aO6fC90aniKQ==",
              "subType": "04"
            }
          },
          "numPartitions": 1,
          "mappings": {
            "dynamic": false,
            "fields": {
              "caption": {
                "type": "string",
                "indexOptions": "offsets",
                "store": true,
                "norms": "include"
              },
              "hasPerson": {
                "type": "boolean"
              }
              // ... additional mapped fields ...
            }
          },
          "indexFeatureVersion": 4,
          "storedSource": {
            "include": [
              "_id",
              "accessory",
              "animal",
              "appliance",
              "caption",
              "dateCaptured",
              "electronic",
              "food",
              "furniture",
              "hasPerson",
              "height",
              "indoor",
              "kitchen",
              "outdoor",
              "sports",
              "url",
              "vehicle",
              "width"
            ]
          }
        },
        "customerDefinition": {
          "storedSource": {
            "include": [
              "_id",
              "caption",
              "url",
              "height",
              "width",
              "dateCaptured",
              "hasPerson",
              "accessory",
              "animal",
              "appliance",
              "electronic",
              "food",
              "furniture",
              "indoor",
              "kitchen",
              "outdoor",
              "sports",
              "vehicle"
            ]
          }
          // ... additional customer definition fields ...
        }
      }
      // ... additional index catalog entries ...
    ]
  },
  "ok": 1.0
}
```

### MongoT -> mongod: server state update

```json
{
  "update": "serverState",
  "ordered": true,
  "txnNumber": 1,
  "$db": "__mdb_internal_search",
  "updates": [
    {
      "q": {
        "_id": {
          "$oid": "69ec2d86a722ed3e1957e639"
        }
      },
      "u": {
        "_id": {
          "$oid": "69ec2d86a722ed3e1957e639"
        },
        "serverName": "69ec2d86a722ed3e1957e639",
        "lastHeartbeatTs": {
          "$date": "2026-04-27T03:47:29.064Z"
        },
        "ready": false,
        "shutdown": false
      },
      "upsert": true
    }
  ]
}
```

### mongod -> MongoT: server state update response

```json
{
  "n": 1,
  "nModified": 1,
  "ok": 1.0
}
```
