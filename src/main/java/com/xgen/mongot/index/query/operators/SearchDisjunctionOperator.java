package com.xgen.mongot.index.query.operators;

import com.xgen.mongot.index.path.string.StringPath;
import com.xgen.mongot.index.query.scores.Score;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import java.util.List;
import org.bson.BsonValue;

public record SearchDisjunctionOperator(Score score, List<StringPath> paths, List<String> query)
    implements SearchOperator {
  @Override
  public Type getType() {
    return Type.SEARCH;
  }

  public static SearchDisjunctionOperator fromBson(DocumentParser parser)
      throws BsonParseException {
    var score = Operators.parseScore(parser);
    var path = Operators.parseStringPath(parser);
    var query = Operators.parseQuery(parser);
    return new SearchDisjunctionOperator(score, path, query);
  }

  public BsonDocumentBuilder toBuilder() {
    return Operators.documentBuilderWithStringPath(score(), this.paths, this.query);
  }

  @Override
  public BsonValue operatorToBson() {
    return toBuilder().build();
  }
}
