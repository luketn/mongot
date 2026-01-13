package com.xgen.mongot.server.command.management.definition.common;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.index.analyzer.definition.CustomAnalyzerDefinition;
import com.xgen.mongot.index.definition.DocumentFieldDefinition;
import com.xgen.mongot.index.definition.StoredSourceDefinition;
import com.xgen.mongot.index.definition.SynonymMappingDefinition;
import com.xgen.mongot.index.definition.TypeSetDefinition;
import com.xgen.mongot.index.query.sort.Sort;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.Value;
import java.util.List;
import java.util.Optional;
import org.bson.BsonDocument;

/**
 * Represents the user-facing search index definition.
 * https://www.mongodb.com/docs/atlas/atlas-search/index-definitions/#options
 */
public record UserSearchIndexDefinition(
    Optional<String> analyzer,
    Optional<String> searchAnalyzer,
    DocumentFieldDefinition mappings,
    Optional<List<CustomAnalyzerDefinition>> analyzers,
    Optional<StoredSourceDefinition> storedSource,
    Optional<List<TypeSetDefinition>> typeSets,
    Optional<Sort> sort,
    Optional<List<SynonymMappingDefinition>> synonyms,
    int numPartitions)
    implements UserIndexDefinition {
  private static class Fields {
    static final Field.Optional<String> ANALYZER =
        Field.builder("analyzer").stringField().optional().noDefault();
    static final Field.Optional<String> SEARCH_ANALYZER =
        Field.builder("searchAnalyzer").stringField().optional().noDefault();
    static final Field.Required<DocumentFieldDefinition> MAPPINGS =
        Field.builder("mappings")
            .classField(DocumentFieldDefinition::fromBson, DocumentFieldDefinition::fieldTypeToBson)
            .disallowUnknownFields()
            .required();
    static final Field.Optional<List<CustomAnalyzerDefinition>> ANALYZERS =
        Field.builder("analyzers")
            .classField(CustomAnalyzerDefinition::fromBson)
            .disallowUnknownFields()
            .asList()
            .mustHaveUniqueAttribute("name", CustomAnalyzerDefinition::name)
            .optional()
            .noDefault();
    static final Field.Optional<List<TypeSetDefinition>> TYPE_SETS =
        Field.builder("typeSets")
            .listOf(
                Value.builder()
                    .classValue(TypeSetDefinition::fromBson)
                    .disallowUnknownFields()
                    .required())
            .mustHaveUniqueAttribute("name", TypeSetDefinition::name)
            .mustNotBeEmpty()
            .optional()
            .noDefault();
    static final Field.Optional<Sort> SORT =
        Field.builder("sort")
            .classField(Sort::fromBsonAsSort)
            .optional()
            .noDefault();
    static final Field.Optional<List<SynonymMappingDefinition>> SYNONYMS =
        Field.builder("synonyms")
            .classField(SynonymMappingDefinition::fromBson)
            .disallowUnknownFields()
            .asList()
            .optional()
            .noDefault();
  }

  @Override
  public BsonDocument toBson() {
    @Var
    var builder =
        BsonDocumentBuilder.builder()
            .field(Fields.ANALYZER, this.analyzer)
            .field(Fields.SEARCH_ANALYZER, this.searchAnalyzer)
            .field(Fields.MAPPINGS, this.mappings)
            .field(Fields.ANALYZERS, this.analyzers)
            .field(UserIndexDefinition.Fields.STORED_SOURCE, this.storedSource)
            .field(Fields.TYPE_SETS, this.typeSets)
            .field(Fields.SORT, this.sort)
            .field(Fields.SYNONYMS, this.synonyms);

    if (this.numPartitions() != UserIndexDefinition.Fields.NUM_PARTITIONS.getDefaultValue()) {
      builder = builder.field(UserIndexDefinition.Fields.NUM_PARTITIONS, this.numPartitions());
    }
    return builder.build();
  }

  public static UserSearchIndexDefinition fromBson(DocumentParser parser)
      throws BsonParseException {
    return new UserSearchIndexDefinition(
        parser.getField(Fields.ANALYZER).unwrap(),
        parser.getField(Fields.SEARCH_ANALYZER).unwrap(),
        parser.getField(Fields.MAPPINGS).unwrap(),
        parser.getField(Fields.ANALYZERS).unwrap(),
        parser.getField(UserIndexDefinition.Fields.STORED_SOURCE).unwrap(),
        parser.getField(Fields.TYPE_SETS).unwrap(),
        parser.getField(Fields.SORT).unwrap(),
        parser.getField(Fields.SYNONYMS).unwrap(),
        parser.getField(UserIndexDefinition.Fields.NUM_PARTITIONS).unwrap());
  }
}
