package com.xgen.mongot.server.command.management.aic;

import com.xgen.mongot.catalogservice.AuthoritativeIndexCatalog;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.server.command.Command;
import com.xgen.mongot.server.command.management.definition.ListSearchIndexesCommandDefinition;
import com.xgen.mongot.server.command.management.definition.ListSearchIndexesResponseDefinition;
import com.xgen.mongot.server.command.management.definition.ListSearchIndexesResponseDefinition.Cursor;
import com.xgen.mongot.server.command.management.definition.ListSearchIndexesResponseDefinition.IndexEntry;
import com.xgen.mongot.server.command.management.definition.common.CommonDefinitions;
import com.xgen.mongot.server.command.management.definition.common.UserViewDefinition;
import com.xgen.mongot.server.message.MessageUtils;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.mongodb.Errors;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public sealed class AicListSearchIndexesCommand implements Command
    permits AicExtendedListSearchIndexesCommand {

  private static final Logger LOG = LoggerFactory.getLogger(AicListSearchIndexesCommand.class);

  private final AuthoritativeIndexCatalog authoritativeIndexCatalog;

  private final String db;

  private final String collectionName;

  private final UUID collectionUuid;

  private final Optional<UserViewDefinition> view;

  private final ListSearchIndexesCommandDefinition definition;

  AicListSearchIndexesCommand(
      AuthoritativeIndexCatalog authoritativeIndexCatalog,
      String db,
      UUID collectionUuid,
      String collectionName,
      Optional<UserViewDefinition> view,
      ListSearchIndexesCommandDefinition definition) {
    this.authoritativeIndexCatalog = authoritativeIndexCatalog;
    this.db = db;
    this.collectionUuid = collectionUuid;
    this.collectionName = collectionName;
    this.view = view;
    this.definition = definition;
  }

  @Override
  public String name() {
    return ListSearchIndexesCommandDefinition.NAME;
  }

  @Override
  public BsonDocument run() {
    LOG.atTrace()
        .addKeyValue("command", ListSearchIndexesCommandDefinition.NAME)
        .addKeyValue("db", this.db)
        .addKeyValue("collectionName", this.collectionName)
        .addKeyValue("viewName", this.view.map(UserViewDefinition::name))
        .log("Received command");

    Stream<IndexDefinition> matchingIndexes = findMatchingIndexes();
    List<BsonDocument> responseData = populateResponseData(matchingIndexes);

    String namespace = String.format("%s.%s", this.db, this.collectionName);
    BsonDocument response =
        new ListSearchIndexesResponseDefinition(
                CommonDefinitions.OK_SUCCESS_CODE, new Cursor(namespace, responseData))
            .toBson();

    if (BsonUtils.isOversized(response)) {
      return MessageUtils.createError(
          Errors.INDEX_INFORMATION_TOO_LARGE,
          "The requested indexes are too large. Try narrowing your filter.");
    }

    return response;
  }

  Stream<IndexDefinition> findMatchingIndexes() {
    Predicate<IndexDefinition> indexDefinitionMatchesTarget =
        idx ->
            this.definition.target().indexId().map(idx.getIndexId()::equals).orElse(true)
                && this.definition.target().indexName().map(idx.getName()::equals).orElse(true);
    return this.authoritativeIndexCatalog.listIndexes(this.collectionUuid).stream()
        .filter(indexDefinitionMatchesTarget);
  }

  List<BsonDocument> populateResponseData(Stream<IndexDefinition> matchingIndexes) {
    return matchingIndexes.map(IndexEntry::fromIndexDefinition).map(IndexEntry::toBson).toList();
  }

  AuthoritativeIndexCatalog getAuthoritativeIndexCatalog() {
    return this.authoritativeIndexCatalog;
  }

  @Override
  public ExecutionPolicy getExecutionPolicy() {
    return ExecutionPolicy.ASYNC;
  }
}
