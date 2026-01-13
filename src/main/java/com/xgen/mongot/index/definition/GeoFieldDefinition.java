package com.xgen.mongot.index.definition;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import org.bson.BsonDocument;

/**
 * GeoFieldDefinition defines how to index fields of type "geo".
 *
 * <p>Configuration includes whether or not to index shapes.
 */
public record GeoFieldDefinition(boolean indexShapes) implements FieldTypeDefinition {

  public static class Fields {
    public static final Field.WithDefault<Boolean> INDEX_SHAPES =
        Field.builder("indexShapes").booleanField().optional().withDefault(false);
  }

  static GeoFieldDefinition fromBson(DocumentParser parser) throws BsonParseException {
    return new GeoFieldDefinition(parser.getField(Fields.INDEX_SHAPES).unwrap());
  }

  @Override
  public BsonDocument fieldTypeToBson() {
    return BsonDocumentBuilder.builder().field(Fields.INDEX_SHAPES, this.indexShapes).build();
  }

  @Override
  public Type getType() {
    return Type.GEO;
  }
}
