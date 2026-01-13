package com.xgen.mongot.index.query;

import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Encodable;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.FieldPathField;
import org.bson.BsonDocument;

public record ReturnScope(FieldPath path) implements Encodable {

  static final class Fields {
    static final Field.Required<FieldPath> PATH =
        Field.builder("path").classField(FieldPathField::parse, FieldPathField::encode).required();
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder().field(Fields.PATH, this.path).build();
  }

  public static ReturnScope fromBson(DocumentParser parser) throws BsonParseException {
    return new ReturnScope(parser.getField(Fields.PATH).unwrap());
  }
}
