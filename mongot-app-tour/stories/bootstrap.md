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

These JSON documents use representative values. The command shapes match the code paths above.

### MongoT Java driver -> mongod: initial hello

```json
{
  "hello": 1,
  "helloOk": true,
  "client": {
    "application": {
      "name": "mongot"
    },
    "driver": {
      "name": "mongo-java-driver|sync",
      "version": "local"
    }
  },
  "$db": "admin"
}
```

### mongod -> MongoT Java driver: initial hello response

```json
{
  "isWritablePrimary": true,
  "topologyVersion": {
    "processId": {
      "$oid": "66f10000000000000000a001"
    },
    "counter": {
      "$numberLong": "6"
    }
  },
  "hosts": ["localhost:27017"],
  "setName": "rs0",
  "setVersion": 1,
  "maxBsonObjectSize": 16777216,
  "maxMessageSizeBytes": 48000000,
  "maxWriteBatchSize": 100000,
  "localTime": {
    "$date": "2026-04-27T00:00:00Z"
  },
  "logicalSessionTimeoutMinutes": 30,
  "connectionId": 42,
  "minWireVersion": 0,
  "maxWireVersion": 25,
  "readOnly": false,
  "ok": 1
}
```

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
  "version": "8.0.0",
  "versionArray": [8, 0, 0, 0],
  "gitVersion": "example",
  "modules": [],
  "allocator": "tcmalloc",
  "javascriptEngine": "mozjs",
  "sysInfo": "deprecated",
  "versionAllocator": "tcmalloc",
  "ok": 1
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
    "term": 3,
    "members": [
      {
        "_id": 0,
        "host": "localhost:27017",
        "priority": 1,
        "votes": 1
      }
    ],
    "settings": {
      "chainingAllowed": true
    }
  },
  "ok": 1
}
```

### MongoT -> mongod: list database collections

```json
{
  "listCollections": 1,
  "filter": {},
  "cursor": {},
  "$db": "sample_mflix"
}
```

### mongod -> MongoT: list database collections response

```json
{
  "cursor": {
    "id": {
      "$numberLong": "0"
    },
    "ns": "sample_mflix.$cmd.listCollections",
    "firstBatch": [
      {
        "name": "movies",
        "type": "collection",
        "options": {},
        "info": {
          "readOnly": false,
          "uuid": {
            "$uuid": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
          }
        },
        "idIndex": {
          "v": 2,
          "key": {
            "_id": 1
          },
          "name": "_id_"
        }
      },
      {
        "name": "movie_search_view",
        "type": "view",
        "options": {
          "viewOn": "movies",
          "pipeline": [
            {
              "$project": {
                "title": 1,
                "plot": 1
              }
            }
          ]
        }
      }
    ]
  },
  "ok": 1
}
```

### MongoT -> mongod: list authoritative search index catalog entries

```json
{
  "find": "indexCatalog",
  "filter": {},
  "$db": "__mdb_internal_search"
}
```

### mongod -> MongoT: authoritative search index catalog response

```json
{
  "cursor": {
    "id": {
      "$numberLong": "0"
    },
    "ns": "__mdb_internal_search.indexCatalog",
    "firstBatch": [
      {
        "_id": {
          "collectionUuid": {
            "$uuid": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
          },
          "indexName": "default"
        },
        "indexId": {
          "$oid": "66f100000000000000000100"
        },
        "schemaVersion": 1,
        "definition": {
          "type": "search",
          "indexID": {
            "$oid": "66f100000000000000000100"
          },
          "collectionUUID": {
            "$uuid": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
          },
          "lastObservedCollectionName": "movies",
          "name": "default",
          "database": "sample_mflix",
          "mappings": {
            "dynamic": true
          },
          "storedSource": {
            "include": ["title", "plot"]
          }
        },
        "customerDefinition": {
          "mappings": {
            "dynamic": true
          },
          "storedSource": {
            "include": ["title", "plot"]
          }
        }
      }
    ]
  },
  "ok": 1
}
```
