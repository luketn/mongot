package com.xgen.mongot.index.query.operators;

import com.xgen.mongot.index.path.string.UnresolvedStringPath;
import com.xgen.mongot.index.query.scores.Score;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import java.util.List;

public record WildcardOperator(
    Score score, List<UnresolvedStringPath> paths, List<String> query, boolean allowAnalyzedField)
    implements TermLevelOperator {
  public static WildcardOperator fromBson(DocumentParser parser) throws BsonParseException {
    return new WildcardOperator(
        Operators.parseScore(parser),
        Operators.parseUnresolvedStringPath(parser),
        Operators.parseQuery(parser),
        TermLevelOperator.parseAllowAnalyzedField(parser));
  }

  @Override
  public Type getType() {
    return Type.WILDCARD;
  }
}
