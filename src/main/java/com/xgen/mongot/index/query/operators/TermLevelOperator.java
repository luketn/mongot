package com.xgen.mongot.index.query.operators;

import com.xgen.mongot.index.path.string.UnresolvedStringPath;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.List;
import org.bson.BsonValue;

public sealed interface TermLevelOperator extends Operator permits RegexOperator, WildcardOperator {

  class Fields {
    public static final Field.WithDefault<Boolean> ALLOW_ANALYZED_FIELD =
        Field.builder("allowAnalyzedField").booleanField().optional().withDefault(false);
  }

  static boolean parseAllowAnalyzedField(DocumentParser parser) throws BsonParseException {
    return parser.getField(Fields.ALLOW_ANALYZED_FIELD).unwrap();
  }

  @Override
  default BsonValue operatorToBson() {
    return Operators.documentBuilderWithUnresolvedStringPath(score(), paths(), query())
        .field(Fields.ALLOW_ANALYZED_FIELD, allowAnalyzedField())
        .build();
  }

  List<UnresolvedStringPath> paths();

  List<String> query();

  boolean allowAnalyzedField();
}
