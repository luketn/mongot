package com.xgen.mongot.index.definition;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import org.bson.BsonDocument;

public record SynonymMappingDefinition(String name, SynonymSourceDefinition source, String analyzer)
    implements DocumentEncodable {
  public static class Fields {
    static final Field.Required<String> NAME =
        Field.builder("name").stringField().mustNotBeEmpty().required();

    static final Field.Required<SynonymSourceDefinition> SOURCE =
        Field.builder("source")
            .classField(SynonymSourceDefinition::fromBson)
            .disallowUnknownFields()
            .required();

    static final Field.Required<String> ANALYZER =
        Field.builder("analyzer").stringField().mustNotBeEmpty().required();
  }

  /** Create a SynonymMappingDefinition from a DocumentParser. */
  @VisibleForTesting
  public static SynonymMappingDefinition fromBson(DocumentParser parser) throws BsonParseException {
    return new SynonymMappingDefinition(
        parser.getField(Fields.NAME).unwrap(),
        parser.getField(Fields.SOURCE).unwrap(),
        parser.getField(Fields.ANALYZER).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.NAME, this.name)
        .field(Fields.SOURCE, this.source)
        .field(Fields.ANALYZER, this.analyzer)
        .build();
  }
}
