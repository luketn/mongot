package com.xgen.mongot.catalogservice;

import com.google.common.annotations.VisibleForTesting;
import com.mongodb.client.MongoClient;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import java.util.List;
import java.util.stream.Stream;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultIndexStats implements IndexStats {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultIndexStats.class);

  @VisibleForTesting protected static final String COLLECTION_NAME = "indexStats";

  private final MetadataClient<IndexStatsEntry> client;

  @VisibleForTesting
  DefaultIndexStats(MetadataClient<IndexStatsEntry> client) {
    this.client = client;
  }

  @Override
  public void insert(IndexStatsEntry indexStats) throws MetadataServiceException {
    this.client.insert(indexStats);
  }

  @Override
  public void delete(IndexStatsEntry.IndexStatsKey key) throws MetadataServiceException {
    this.client.delete(IndexStatsEntry.keyAsBson(key));
  }

  @Override
  public List<IndexStatsEntry> list(BsonDocument filter) throws MetadataServiceException {
    return this.client.list(filter).stream()
        .flatMap(
            d -> {
              try {
                return Stream.of(IndexStatsEntry.fromBson(BsonDocumentParser.fromRoot(d).build()));
              } catch (BsonParseException e) {
                LOG.atWarn()
                    .addKeyValue("document", d)
                    .setCause(e)
                    .log("Ignoring unparsed document from Index Stats Catalog");
                return Stream.empty();
              }
            })
        .toList();
  }

  @Override
  public List<IndexStatsEntry> list() throws MetadataServiceException {
    return list(new BsonDocument());
  }

  @Override
  public void createCollectionIndexes() throws MetadataServiceException {
    this.client.createIndex(
        Indexes.ascending(
            String.format(
                "%s.%s",
                IndexStatsEntry.Fields.INDEX_STATS_KEY.getName(),
                IndexStatsEntry.IndexStatsKey.Fields.INDEX_ID.getName())),
        new IndexOptions().background(true));

    this.client.createIndex(
        Indexes.ascending(
            String.format(
                "%s.%s",
                IndexStatsEntry.Fields.INDEX_STATS_KEY.getName(),
                IndexStatsEntry.IndexStatsKey.Fields.SERVER_ID.getName())),
        new IndexOptions().background(true));
  }

  public static DefaultIndexStats create(MongoClient mongoClient) {
    return new DefaultIndexStats(new MetadataClient<>(mongoClient, COLLECTION_NAME));
  }
}
