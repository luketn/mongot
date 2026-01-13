package com.xgen.mongot.server.command.management;

import com.xgen.mongot.server.command.management.definition.SearchIndexCommandDefinition;
import com.xgen.mongot.server.command.management.definition.common.UserViewDefinition;
import java.util.Optional;
import java.util.UUID;

public record IndexManagementCommandContext<T extends SearchIndexCommandDefinition>(
    String dbName,
    UUID collectionUuid,
    String collectionName,
    Optional<UserViewDefinition> view,
    T definition) {}
