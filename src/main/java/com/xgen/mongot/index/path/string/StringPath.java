package com.xgen.mongot.index.path.string;

import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Encodable;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;

public abstract class StringPath implements Encodable {

  private static final String EXPECTED_TYPE = "document or string";

  public static final class Fields {
    private static final Field.Required<String> VALUE =
        Field.builder("value").stringField().required();

    private static final Field.Optional<String> MULTI =
        Field.builder("multi")
            .stringField()
            .validate(s -> s.contains(".") ? Optional.of("cannot contain \".\"") : Optional.empty())
            .optional()
            .noDefault();
  }

  public enum Type {
    FIELD,
    MULTI_FIELD,
  }

  public abstract Type getType();

  /** Constructs a StringPath from the supplied BsonValue. */
  public static StringPath fromBson(BsonParseContext context, BsonValue bsonValue)
      throws BsonParseException {
    switch (bsonValue.getBsonType()) {
      case STRING:
        FieldPath fieldPath = FieldPath.parse(bsonValue.asString().getValue());
        return new StringFieldPath(fieldPath);

      case DOCUMENT:
        try (var parser = BsonDocumentParser.withContext(context, bsonValue.asDocument()).build()) {
          return fromBsonDocument(parser);
        }

      default:
        return context.handleUnexpectedType(EXPECTED_TYPE, bsonValue.getBsonType());
    }
  }

  /** Constructs a StringPath from a document. */
  public static StringPath fromBsonDocument(DocumentParser parser) throws BsonParseException {
    FieldPath fieldPath = FieldPath.parse(parser.getField(Fields.VALUE).unwrap());
    Optional<String> multi = parser.getField(Fields.MULTI).unwrap();

    return multi
        .map(m -> (StringPath) new StringMultiFieldPath(fieldPath, m))
        .orElseGet(() -> new StringFieldPath(fieldPath));
  }

  public static List<FieldPath> toFieldPaths(List<StringPath> stringPaths) {
    return stringPaths.stream()
        .map(
            stringPath -> {
              return switch (stringPath.getType()) {
                case FIELD -> stringPath.asField().getValue();
                case MULTI_FIELD -> stringPath.asMultiField().getFieldPath();
              };
            })
        .collect(Collectors.toList());
  }

  public FieldPath getBaseFieldPath() {
    return switch (getType()) {
      case FIELD -> asField().getValue();
      case MULTI_FIELD -> asMultiField().getFieldPath();
    };
  }

  public boolean isField() {
    return getType() == Type.FIELD;
  }

  public boolean isMultiField() {
    return getType() == Type.MULTI_FIELD;
  }

  public StringFieldPath asField() {
    Check.expectedType(Type.FIELD, getType());
    return (StringFieldPath) this;
  }

  public StringMultiFieldPath asMultiField() {
    Check.expectedType(Type.MULTI_FIELD, getType());
    return (StringMultiFieldPath) this;
  }

  @Override
  public BsonValue toBson() {
    return switch (getType()) {
      case FIELD -> fieldPathToBson();
      case MULTI_FIELD -> multiFieldPathToBson();
    };
  }

  private BsonString fieldPathToBson() {
    FieldPath path = this.asField().getValue();
    return new BsonString(path.toString());
  }

  private BsonDocument multiFieldPathToBson() {
    StringMultiFieldPath multiFieldPath = asMultiField();
    return BsonDocumentBuilder.builder()
        .field(Fields.VALUE, multiFieldPath.getFieldPath().toString())
        .field(Fields.MULTI, Optional.of(multiFieldPath.getMulti()))
        .build();
  }
}
