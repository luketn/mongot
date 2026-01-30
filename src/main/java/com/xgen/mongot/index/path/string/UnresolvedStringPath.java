package com.xgen.mongot.index.path.string;

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
import org.bson.BsonString;
import org.bson.BsonValue;

public abstract sealed class UnresolvedStringPath implements Encodable
    permits UnresolvedStringFieldPath,
        UnresolvedStringMultiFieldPath,
        UnresolvedStringWildcardPath {
  private static final String EXPECTED_TYPE = "document or string";

  public static final class Fields {
    private static final Field.Optional<String> VALUE =
        Field.builder("value").stringField().optional().noDefault();

    private static final Field.Optional<String> WILDCARD =
        Field.builder("wildcard")
            .stringField()
            .validate(UnresolvedStringPath::containsWildcard)
            .validate(UnresolvedStringPath::adjacentWildcards)
            .optional()
            .noDefault();

    private static final Field.Optional<String> MULTI =
        Field.builder("multi")
            .stringField()
            .validate(s -> s.contains(".") ? Optional.of("cannot contain \".\"") : Optional.empty())
            .optional()
            .noDefault();
  }

  /** Constructs an UnresolvedStringPath from a BsonValue. */
  public static UnresolvedStringPath fromBson(BsonParseContext context, BsonValue bsonValue)
      throws BsonParseException {
    FieldPath path;
    switch (bsonValue.getBsonType()) {
      case STRING -> {
        path = FieldPath.parse(bsonValue.asString().getValue());
        return new UnresolvedStringFieldPath(path);
      }
      case DOCUMENT -> {
        try (var parser = BsonDocumentParser.withContext(context, bsonValue.asDocument()).build()) {
          return handlePathAsDocument(parser);
        }
      }
      default -> {
        return context.handleUnexpectedType(EXPECTED_TYPE, bsonValue.getBsonType());
      }
    }
  }

  public static List<FieldPath> toFieldPaths(List<UnresolvedStringPath> stringPaths) {
    return stringPaths.stream()
        .map(
            unresolvedStringPath -> {
              return switch (unresolvedStringPath) {
                case UnresolvedStringFieldPath fieldPath -> fieldPath.getValue();
                case UnresolvedStringMultiFieldPath multiFieldPath -> multiFieldPath.getFieldPath();
                case UnresolvedStringWildcardPath wildcardPath ->
                    // FieldPath ancestor comparison works with wildcard paths
                    FieldPath.parse(wildcardPath.getValue());
              };
            })
        .collect(Collectors.toList());
  }

  @Override
  public BsonValue toBson() {
    return switch (this) {
      case UnresolvedStringFieldPath fieldPath -> new BsonString(fieldPath.getValue().toString());
      case UnresolvedStringMultiFieldPath multiFieldPath ->
          BsonDocumentBuilder.builder()
              .field(
                  UnresolvedStringPath.Fields.VALUE,
                  Optional.of(multiFieldPath.getFieldPath().toString()))
              .field(UnresolvedStringPath.Fields.MULTI, Optional.of(multiFieldPath.getMulti()))
              .build();
      case UnresolvedStringWildcardPath wildcardPath ->
          BsonDocumentBuilder.builder()
              .field(UnresolvedStringPath.Fields.WILDCARD, Optional.of(wildcardPath.toString()))
              .build();
    };
  }

  public static Optional<String> containsWildcard(String s) {
    return s.contains("*") ? Optional.empty() : Optional.of("must contain a \"*\"");
  }

  public static Optional<String> adjacentWildcards(String s) {
    return s.contains("**")
        ? Optional.of("cannot contain more than 1 \"*\" next to each other")
        : Optional.empty();
  }

  private static UnresolvedStringPath handlePathAsDocument(DocumentParser parser)
      throws BsonParseException {
    var wildcardField = parser.getField(UnresolvedStringPath.Fields.WILDCARD);
    Optional<String> multi = parser.getField(UnresolvedStringPath.Fields.MULTI).unwrap();

    return wildcardField.unwrap().isPresent()
        ? handleWildcardField(wildcardField.unwrap().get(), multi.isPresent(), parser.getContext())
        : fromStringPath(StringPath.fromBsonDocument(parser));
  }

  private static UnresolvedStringWildcardPath handleWildcardField(
      String wildcardPath, boolean multiPresent, BsonParseContext context)
      throws BsonParseException {
    if (multiPresent) {
      context.handleSemanticError("multi cannot be defined along with wildcard");
    }

    return new UnresolvedStringWildcardPath(wildcardPath);
  }

  private static UnresolvedStringPath fromStringPath(StringPath path) {
    return switch (path.getType()) {
      case FIELD -> new UnresolvedStringFieldPath(path.asField().getValue());
      case MULTI_FIELD -> {
        StringMultiFieldPath multiFieldPath = path.asMultiField();
        yield new UnresolvedStringMultiFieldPath(
            multiFieldPath.getFieldPath(), multiFieldPath.getMulti());
      }
    };
  }
}
