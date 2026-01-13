package com.xgen.mongot.catalogservice;

import java.util.List;
import org.bson.BsonDocument;
import org.bson.types.ObjectId;

public interface ServerState {

  /** Inserts a new server state entry into the metadata collection */
  void insert(ServerStateEntry indexStats) throws MetadataServiceException;

  /**
   * Deletes a server state entry from the metadata collection.
   *
   * @param serverId the id of the server to delete
   * @throws MetadataServiceException if there was an error deleting the entry
   */
  void delete(ObjectId serverId) throws MetadataServiceException;

  /** Lists the server state entries from the metadata collection based on a filter */
  List<ServerStateEntry> list(BsonDocument filter) throws MetadataServiceException;

  /** Lists all server state entries from the metadata collection */
  List<ServerStateEntry> list() throws MetadataServiceException;
}
