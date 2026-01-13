package com.xgen.mongot.index.query.scores;

import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.ClassField;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.List;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonValue;

public sealed interface Score extends DocumentEncodable
    permits ConstantScore,
        DismaxScore,
        EmbeddedScore,
        FunctionScore,
        PathBoostScore,
        ValueBoostScore {

  class Fields {
    public static final Field.Optional<Score> BOOST =
        Fields.build("boost", Score::handleBoostField);

    public static final Field.Optional<ConstantScore> CONSTANT =
        Fields.build("constant", ConstantScore::fromBson);

    public static final Field.Optional<DismaxScore> DISMAX =
        Fields.build("dismax", DismaxScore::fromBson);

    public static final Field.Optional<FunctionScore> FUNCTION =
        Fields.build("function", FunctionScore::fromBson);

    public static final Field.Optional<EmbeddedScore> EMBEDDED =
        Fields.build("embedded", EmbeddedScore::fromBson);

    private static <T extends Score> Field.Optional<T> build(
        String name, ClassField.FromDocumentParser<T> parser) {
      return Field.builder(name)
          .classField(parser, Score::scoreToBson)
          .disallowUnknownFields()
          .optional()
          .noDefault();
    }
  }

  enum Type {
    VALUE_BOOST,
    PATH_BOOST,
    CONSTANT,
    DISMAX,
    FUNCTION,
    EMBEDDED
  }

  Type getType();

  List<FieldPath> getChildPaths();

  BsonValue scoreToBson();

  /** default score for any operator. */
  static Score defaultScore() {
    // Referencing BoostScore from a static field here
    // can cause initializer deadlock, see CLOUDP-54926.
    return ValueBoostScore.DEFAULT_BOOST_DEFINITION;
  }

  static Score fromBson(DocumentParser parser) throws BsonParseException {
    var boost = parser.getField(Fields.BOOST);
    var constant = parser.getField(Fields.CONSTANT);
    var function = parser.getField(Fields.FUNCTION);

    return parser.getGroup().exactlyOneOf(boost, constant, function);
  }

  static Score fromBsonAllowDismax(DocumentParser parser) throws BsonParseException {
    var boost = parser.getField(Fields.BOOST);
    var constant = parser.getField(Fields.CONSTANT);
    var function = parser.getField(Fields.FUNCTION);
    var dismax = parser.getField(Fields.DISMAX);

    return parser.getGroup().exactlyOneOf(boost, constant, function, dismax);
  }

  static Score fromBsonAllowEmbedded(DocumentParser parser) throws BsonParseException {
    var boost = parser.getField(Fields.BOOST);
    var constant = parser.getField(Fields.CONSTANT);
    var function = parser.getField(Fields.FUNCTION);
    var embedded = parser.getField(Fields.EMBEDDED);

    return parser.getGroup().exactlyOneOf(boost, constant, function, embedded);
  }

  @Override
  default BsonDocument toBson() {
    BsonDocumentBuilder builder = BsonDocumentBuilder.builder();
    return switch (this) {
      case ValueBoostScore valueBoostScore ->
          builder.field(Fields.BOOST, Optional.of(valueBoostScore)).build();
      case PathBoostScore pathBoostScore ->
          builder.field(Fields.BOOST, Optional.of(pathBoostScore)).build();
      case ConstantScore constantScore ->
          builder.field(Fields.CONSTANT, Optional.of(constantScore)).build();
      case DismaxScore dismaxScore ->
          builder.field(Fields.DISMAX, Optional.of(dismaxScore)).build();
      case FunctionScore functionScore ->
          builder.field(Fields.FUNCTION, Optional.of(functionScore)).build();
      case EmbeddedScore embeddedScore ->
          builder.field(Fields.EMBEDDED, Optional.of(embeddedScore)).build();
    };
  }

  private static Score handleBoostField(DocumentParser parser) throws BsonParseException {
    var value = parser.getField(ValueBoostScore.Fields.VALUE).unwrap();
    var path = parser.getField(PathBoostScore.Fields.PATH).unwrap();
    var undefined = parser.getField(PathBoostScore.Fields.UNDEFINED);

    parser
        .getGroup()
        .exactlyOneOf(
            parser.getField(ValueBoostScore.Fields.VALUE),
            parser.getField(PathBoostScore.Fields.PATH));

    boolean valueAndUndefined = value.isPresent() && undefined.isPresent();
    if (valueAndUndefined) {
      parser
          .getContext()
          .handleSemanticError("\"undefined\" cannot be present when \"value\" is present");
    }

    if (value.isPresent()) {
      return new ValueBoostScore(value.get());
    }

    return new PathBoostScore(
        FieldPath.parse(
            path.orElseThrow(
                () -> new AssertionError("either one of \"path\" or \"value\" must be present"))),
        undefined.unwrap());
  }
}
