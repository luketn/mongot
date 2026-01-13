package com.xgen.mongot.index.analyzer.custom;

import com.google.common.collect.ImmutableMap;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Map;
import java.util.Objects;
import org.bson.BsonDocument;

public class MappingCharFilterDefinition extends CharFilterDefinition {
  static class Fields {
    static final Field.Required<Map<String, String>> MAPPINGS =
        Field.builder("mappings")
            .stringField()
            .asMap()
            .mustNotBeEmpty()
            .mustNotContainEmptyStringAsKey()
            .required();
  }

  public final ImmutableMap<String, String> mappings;

  public MappingCharFilterDefinition(Map<String, String> mappings) {
    this.mappings = ImmutableMap.copyOf(mappings);
  }

  public static MappingCharFilterDefinition fromBson(DocumentParser parser)
      throws BsonParseException {
    return new MappingCharFilterDefinition(parser.getField(Fields.MAPPINGS).unwrap());
  }

  @Override
  public Type getType() {
    return Type.MAPPING;
  }

  @Override
  BsonDocument charFilterToBson() {
    return BsonDocumentBuilder.builder().field(Fields.MAPPINGS, this.mappings).build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MappingCharFilterDefinition that = (MappingCharFilterDefinition) o;
    return this.mappings.equals(that.mappings);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.mappings);
  }
}
