package com.xgen.mongot.index.definition;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.Encodable;
import com.xgen.mongot.util.bson.parser.Field;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonValue;

public sealed interface DynamicDefinition extends Encodable
    permits DynamicDefinition.Boolean, DynamicDefinition.Document {
  DynamicDefinition DISABLED = new Boolean(false);

  static DynamicDefinition fromBson(BsonParseContext context, BsonValue bsonValue)
      throws BsonParseException {
    return switch (bsonValue.getBsonType()) {
      case BOOLEAN -> new Boolean(bsonValue.asBoolean().getValue());
      case DOCUMENT -> Document.parse(context, bsonValue.asDocument());
      default -> context.handleUnexpectedType("boolean or document", bsonValue.getBsonType());
    };
  }

  default boolean isEnabled() {
    return !this.equals(DynamicDefinition.DISABLED);
  }

  record Boolean(boolean value) implements DynamicDefinition {
    @Override
    public BsonValue toBson() {
      return new BsonBoolean(this.value);
    }
  }

  record Document(String typeSet) implements DynamicDefinition {
    private static class Fields {
      private static final Field.Required<String> TYPE_SET =
          Field.builder("typeSet").stringField().mustNotBeBlank().required();
    }

    static Document parse(BsonParseContext context, BsonDocument document)
        throws BsonParseException {
      try (var parser =
          BsonDocumentParser.withContext(context, document).allowUnknownFields(false).build()) {
        return new Document(parser.getField(Fields.TYPE_SET).unwrap());
      }
    }

    @Override
    public BsonValue toBson() {
      return BsonDocumentBuilder.builder().field(Fields.TYPE_SET, this.typeSet).build();
    }
  }
}
