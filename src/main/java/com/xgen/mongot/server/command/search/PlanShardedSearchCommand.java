package com.xgen.mongot.server.command.search;

import com.xgen.mongot.index.query.SearchQuery;
import com.xgen.mongot.server.command.Command;
import com.xgen.mongot.server.command.CommandFactory;
import com.xgen.mongot.server.command.search.definition.PlanShardedSearchCommandResponseDefinition;
import com.xgen.mongot.server.command.search.definition.request.PlanShardedSearchCommandDefinition;
import com.xgen.mongot.server.message.MessageUtils;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PlanShardedSearchCommand implements Command {

  private static final Logger LOG = LoggerFactory.getLogger(PlanShardedSearchCommand.class);

  @SuppressWarnings("unused")
  private final PlanShardedSearchCommandDefinition definition;

  PlanShardedSearchCommand(PlanShardedSearchCommandDefinition definition) {
    this.definition = definition;
  }

  @Override
  public String name() {
    return PlanShardedSearchCommandDefinition.NAME;
  }

  @Override
  public BsonDocument run() {
    LOG.atTrace()
        .addKeyValue("command", PlanShardedSearchCommandDefinition.NAME)
        .log("Received command");

    try {
      SearchQuery queryDefinition = SearchQuery.fromBson(this.definition.queryDocument());
      PlanShardedSearchCommandResponseDefinition.ShardedSearchPlan shardedSearchPlan =
          ShardedSearchPlanner.planSearch(queryDefinition, this.definition.searchFeatures());

      return PlanShardedSearchCommandResponseDefinition.create(shardedSearchPlan).toBson();
    } catch (ShardedSearchPlannerException | BsonParseException e) {
      LOG.info("Invalid ShardedSearchPlanDefinition:", e);
      return MessageUtils.createErrorBody(e);
    }
  }

  @Override
  public ExecutionPolicy getExecutionPolicy() {
    return ExecutionPolicy.SYNC;
  }

  public static class Factory implements CommandFactory {

    @Override
    public Command create(BsonDocument doc) {
      try (var parser = BsonDocumentParser.fromRoot(doc).allowUnknownFields(true).build()) {
        PlanShardedSearchCommandDefinition definition =
            PlanShardedSearchCommandDefinition.fromBson(parser);
        return new PlanShardedSearchCommand(definition);
      } catch (BsonParseException e) {
        // we have no way of throwing checked exceptions beyond this method
        // (called directly by opmsg)
        throw new IllegalArgumentException(e.getMessage());
      }
    }
  }
}
