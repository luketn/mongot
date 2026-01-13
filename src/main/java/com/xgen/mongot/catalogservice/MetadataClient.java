package com.xgen.mongot.catalogservice;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.IndexOptions;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import java.util.List;
import java.util.stream.StreamSupport;
import org.bson.BsonDocument;
import org.bson.conversions.Bson;

/**
 * The MetadataClient exposes all the functionality required to perform CRUD queries on any of the
 * internal mongod metadata collections.
 */
public class MetadataClient<T extends DocumentEncodable> {
  protected static final String DATABASE_NAME = "__mdb_internal_search";

  private final MongoClient mongoClient;
  private final String collectionName;

  public MetadataClient(MongoClient mongoClient, String collectionName) {
    this.mongoClient = mongoClient;
    this.collectionName = collectionName;
  }

  /**
   * Inserts the index entry into the metadata collection.
   *
   * @param document the document to be inserted
   * @throws MetadataServiceException if there was an error persisting the entry to the collection,
   *     for example, if an index with the same ID already exists in the collection
   */
  public synchronized void insert(T document) throws MetadataServiceException {
    MetadataServiceException.wrapIfThrows(() -> this.getCollection().insertOne(document.toBson()));
  }

  /**
   * Replaces an existing index entry in the collection with a new value.
   *
   * @param filter filter for the document to replace
   * @param document the document to be inserted
   * @throws MetadataServiceException if there was an error persisting the entry to the collection
   */
  public synchronized void replace(Bson filter, T document) throws MetadataServiceException {
    MetadataServiceException.wrapIfThrows(
        () -> this.getCollection().replaceOne(filter, document.toBson()));
  }

  /**
   * Deletes a single item in the metadata collection with the given filter.
   *
   * @param filter filter for the document to delete
   * @throws MetadataServiceException if there was an error deleting the specified document
   */
  public synchronized void delete(Bson filter) throws MetadataServiceException {
    MetadataServiceException.wrapIfThrows(() -> this.getCollection().deleteOne(filter));
  }

  /**
   * Returns all documents in the metadata collection matching the given filter.
   *
   * @param filter filter to apply to the list command
   */
  public synchronized List<BsonDocument> list(BsonDocument filter) {
    return StreamSupport.stream(this.getCollection().find(filter).spliterator(), false).toList();
  }

  /**
   * Creates a new index on the collection if the index does not yet exist. This command is
   * idempotent and does nothing if the index already exits.
   *
   * @param keys the collection keys to index
   * @param indexOptions options on how to bild the index
   * @throws MetadataServiceException any error that may be thrown by the mongodb client
   */
  public synchronized void createIndex(Bson keys, IndexOptions indexOptions)
      throws MetadataServiceException {
    MetadataServiceException.wrapIfThrows(
        () -> this.getCollection().createIndex(keys, indexOptions));
  }

  private MongoCollection<BsonDocument> getCollection() {
    return this.mongoClient
        .getDatabase(DATABASE_NAME)
        .getCollection(this.collectionName, BsonDocument.class);
  }
}
