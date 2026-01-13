package com.xgen.testing.mongot.server.command.management.definition;

import com.xgen.mongot.index.definition.DocumentFieldDefinition;
import com.xgen.mongot.index.definition.TypeSetDefinition;
import com.xgen.mongot.index.query.sort.Sort;
import com.xgen.mongot.server.command.management.definition.common.UserSearchIndexDefinition;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import java.util.List;
import java.util.Optional;

public class UserSearchIndexDefinitionBuilder {
  private Optional<String> analyzer = Optional.empty();

  private Optional<String> searchAnalyzer = Optional.empty();

  private DocumentFieldDefinition mappings = DocumentFieldDefinitionBuilder.builder().build();

  private int numPartitions = 1;

  private Optional<List<TypeSetDefinition>> typeSets = Optional.empty();

  private Optional<Sort> sort = Optional.empty();

  public UserSearchIndexDefinitionBuilder analyzer(String analyzer) {
    this.analyzer = Optional.of(analyzer);
    return this;
  }

  public UserSearchIndexDefinitionBuilder searchAnalyzer(String analyzer) {
    this.searchAnalyzer = Optional.of(analyzer);
    return this;
  }

  public UserSearchIndexDefinitionBuilder mappings(DocumentFieldDefinitionBuilder fields) {
    this.mappings = fields.build();
    return this;
  }

  public UserSearchIndexDefinitionBuilder numPartitions(int numPartitions) {
    this.numPartitions = numPartitions;
    return this;
  }

  public UserSearchIndexDefinitionBuilder typeSets(List<TypeSetDefinition> typeSets) {
    this.typeSets = Optional.of(typeSets);
    return this;
  }

  public UserSearchIndexDefinitionBuilder sort(Sort sort) {
    this.sort = Optional.of(sort);
    return this;
  }

  public UserSearchIndexDefinition build() {
    return new UserSearchIndexDefinition(
        this.analyzer,
        this.searchAnalyzer,
        this.mappings,
        Optional.empty(),
        Optional.empty(),
        this.typeSets,
        this.sort,
        Optional.empty(),
        this.numPartitions);
  }

  public static UserSearchIndexDefinitionBuilder builder() {
    return new UserSearchIndexDefinitionBuilder();
  }
}
