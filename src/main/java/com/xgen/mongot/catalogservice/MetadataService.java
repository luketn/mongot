package com.xgen.mongot.catalogservice;

/**
 * This class is the entry point to the different metadata collections stored in the internal
 * '__mdb_internal_search' database on the cluster's the mongod instance. This is an internal
 * database and the collections are only used by mongot to manage the lifecycle of the deployment.
 */
public interface MetadataService {

  /**
   * Gets the authoritative index catalog metadata collection which stores the authoritative index
   * definitions as requested by the customer.
   *
   * @return an implementation of the AuthoritativeIndexCatalog metadata access class
   */
  AuthoritativeIndexCatalog getAuthoritativeIndexCatalog();

  /**
   * Gets the index stats metadata collection which stores the up-to-date stats of each index per
   * mongot server.
   *
   * @return an implementation of the IndexStats metadata access class
   */
  IndexStats getIndexStats();

  /**
   * Gets the server state metadata collection which stores the active mongot servers in the
   * cluster.
   *
   * @return an implementation of the ServerState metadata access class
   */
  ServerState getServerState();

  /** Close the metadata service and associated mongod clients. */
  void close();
}
