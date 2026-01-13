package com.xgen.mongot.server.command.management;

import com.xgen.mongot.server.command.Command;
import com.xgen.mongot.server.command.management.definition.CreateSearchIndexesCommandDefinition;
import com.xgen.mongot.server.command.management.definition.DropSearchIndexCommandDefinition;
import com.xgen.mongot.server.command.management.definition.ListSearchIndexesCommandDefinition;
import com.xgen.mongot.server.command.management.definition.UpdateSearchIndexCommandDefinition;

public interface ManageSearchIndexCommandFactory {
  Command createSearchIndexesCommand(
      IndexManagementCommandContext<CreateSearchIndexesCommandDefinition> commandContext);

  Command listSearchIndexesCommand(
      IndexManagementCommandContext<ListSearchIndexesCommandDefinition> commandContext);

  Command updateSearchIndexCommand(
      IndexManagementCommandContext<UpdateSearchIndexCommandDefinition> commandContext);

  Command dropSearchIndexCommand(
      IndexManagementCommandContext<DropSearchIndexCommandDefinition> commandContext);
}
