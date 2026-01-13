package com.xgen.testing.mongot.server.command.search.definition.request;

import com.xgen.mongot.server.command.search.definition.request.CursorOptionsDefinition;
import com.xgen.mongot.server.command.search.definition.request.ExplainDefinition;
import com.xgen.mongot.server.command.search.definition.request.OptimizationFlagsDefinition;
import com.xgen.mongot.server.command.search.definition.request.SearchCommandDefinition;
import com.xgen.mongot.util.Check;
import java.util.Optional;
import java.util.UUID;
import org.bson.BsonDocument;

public class SearchCommandDefinitionBuilder {

  private Optional<String> db = Optional.empty();
  private Optional<String> collectionName = Optional.empty();
  private Optional<UUID> collectionUuid = Optional.empty();
  private Optional<String> viewName = Optional.empty();
  private Optional<BsonDocument> query = Optional.empty();
  private Optional<ExplainDefinition> explain = Optional.empty();

  private Optional<Integer> intermediate = Optional.empty();

  private Optional<CursorOptionsDefinition> cursorOptions = Optional.empty();
  private Optional<OptimizationFlagsDefinition> optimizationFlags = Optional.empty();

  public static SearchCommandDefinitionBuilder builder() {
    return new SearchCommandDefinitionBuilder();
  }

  public SearchCommandDefinitionBuilder db(String db) {
    this.db = Optional.of(db);
    return this;
  }

  public SearchCommandDefinitionBuilder collectionName(String collectionName) {
    this.collectionName = Optional.of(collectionName);
    return this;
  }

  public SearchCommandDefinitionBuilder collectionUuid(UUID collectionUuid) {
    this.collectionUuid = Optional.of(collectionUuid);
    return this;
  }

  public SearchCommandDefinitionBuilder viewName(String viewName) {
    this.viewName = Optional.of(viewName);
    return this;
  }

  public SearchCommandDefinitionBuilder query(BsonDocument query) {
    this.query = Optional.of(query);
    return this;
  }

  public SearchCommandDefinitionBuilder explain(ExplainDefinition explain) {
    this.explain = Optional.of(explain);
    return this;
  }

  public SearchCommandDefinitionBuilder intermediate(Integer protocolVersion) {
    this.intermediate = Optional.of(protocolVersion);
    return this;
  }

  public SearchCommandDefinitionBuilder cursorOptions(CursorOptionsDefinition cursorOptions) {
    this.cursorOptions = Optional.of(cursorOptions);
    return this;
  }

  public SearchCommandDefinitionBuilder optimizationFlags(
      OptimizationFlagsDefinition optimizationFlags) {
    this.optimizationFlags = Optional.of(optimizationFlags);
    return this;
  }

  /** Builds the SearchCommandDefinition. */
  public SearchCommandDefinition build() {
    Check.isPresent(this.db, "db");
    Check.isPresent(this.collectionName, "collectionName");
    Check.isPresent(this.collectionUuid, "collectionUuid");
    Check.isPresent(this.query, "query");

    return new SearchCommandDefinition(
        this.db.get(),
        this.collectionName.get(),
        this.collectionUuid.get(),
        this.viewName,
        this.query.get(),
        this.explain,
        this.intermediate,
        this.cursorOptions,
        this.optimizationFlags);
  }
}
