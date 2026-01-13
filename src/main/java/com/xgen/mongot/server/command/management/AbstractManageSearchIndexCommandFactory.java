package com.xgen.mongot.server.command.management;

import com.xgen.mongot.server.command.Command;
import com.xgen.mongot.server.command.CommandFactory;
import com.xgen.mongot.server.command.management.definition.CreateSearchIndexesCommandDefinition;
import com.xgen.mongot.server.command.management.definition.DropSearchIndexCommandDefinition;
import com.xgen.mongot.server.command.management.definition.ListSearchIndexesCommandDefinition;
import com.xgen.mongot.server.command.management.definition.ManageSearchIndexCommandDefinition;
import com.xgen.mongot.server.command.management.definition.SearchIndexCommandDefinition;
import com.xgen.mongot.server.command.management.definition.UpdateSearchIndexCommandDefinition;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import org.bson.BsonDocument;

public abstract class AbstractManageSearchIndexCommandFactory
    implements ManageSearchIndexCommandFactory, CommandFactory {

  @Override
  public Command create(BsonDocument args) {
    try (var parser = BsonDocumentParser.fromRoot(args).allowUnknownFields(true).build()) {
      ManageSearchIndexCommandDefinition definition =
          ManageSearchIndexCommandDefinition.fromBson(parser);

      return switch (definition.userCommand()) {
        case CreateSearchIndexesCommandDefinition createDefinition ->
            createSearchIndexesCommand(createCommandContext(definition, createDefinition));
        case ListSearchIndexesCommandDefinition listDefinition ->
            listSearchIndexesCommand(createCommandContext(definition, listDefinition));
        case UpdateSearchIndexCommandDefinition updateDefinition ->
            updateSearchIndexCommand(createCommandContext(definition, updateDefinition));
        case DropSearchIndexCommandDefinition dropDefinition ->
            dropSearchIndexCommand(createCommandContext(definition, dropDefinition));
      };
    } catch (BsonParseException e) {
      throw new IllegalArgumentException(e.getMessage());
    }
  }

  private static <T extends SearchIndexCommandDefinition>
      IndexManagementCommandContext<T> createCommandContext(
          ManageSearchIndexCommandDefinition manageCommandDefinition, T userCommandDefinition) {
    return new IndexManagementCommandContext<>(
        manageCommandDefinition.db(),
        manageCommandDefinition.collectionUuid(),
        manageCommandDefinition.collectionName(),
        manageCommandDefinition.view(),
        userCommandDefinition);
  }
}
