This directory contains all the files referenced in
the Mongot
Community [Quick Start Guide](https://www.mongodb.com/docs/atlas/atlas-search/tutorial/?deployment-type=self)
to quickly get a MongoDB Community Search running in a local docker container alongside mongod.

The steps listed will generate:

* A MongoDB Community Edition Server (`mongod`) with a single node replica set on port 27017
* A MongoDB Search (`mongot`) search engine component on port 27028
* Persistant data volumes on both ports
* Pre-loaded sample data

## Before You Begin:

* Download Docker v4.40 or higher
* Download Docker Compose
* Download the `curl` command
* Download `mongosh` locally or have access to it through Docker

## Setup

*All steps are expected to be ran from the `community-quick-start` directory.*

1. Give read-only access to the password file:

```shell
chmod 400 ./pwfile
```

**NOTE**: If you would like to change the password update the `pwfile` and `init-mongosh.sh` with
the new password.

2. Download the sample data:

```shell
curl https://atlas-education.s3.amazonaws.com/sampledata.archive -o sampledata.archive
```

3. Create the Docker environment:

```shell
docker network create search-community
```

## Starting `mongod` and `mongot`

1. Launch `mongod` and `mongot`:

```shell
docker compose up -d
```

2. Wait for both servers to fully initialize:

```shell
docker compose logs -f
```

The initialization is complete when the logs return `mongod startup complete`.

## Create a MongoDB Search index

1. Connect to MongoDB with mongosh

```shell
mongosh
```

2. In the MongoDB shell, run the following commands to create a search index on the sample data

```mongodb-json
// Switch to the sample database
use sample_mflix

// Create a search index on the movies collection
db.movies.createSearchIndex(
   "default",
   { mappings:
      { dynamic: true }
   }
)
```

3. Test search functionality:

```mongodb-json
// Search for movies with "baseball" in the plot
db.movies.aggregate( [
   {
      $search: {
         "text": {
         "query": "baseball",
         "path": "plot"
         }
      }
   },
   {
      $limit: 5
   },
   {
      $project: {
         "_id": 0,
         "title": 1,
         "plot": 1
      }
   }
] )
```