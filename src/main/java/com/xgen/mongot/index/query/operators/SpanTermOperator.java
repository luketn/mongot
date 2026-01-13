package com.xgen.mongot.index.query.operators;

import com.xgen.mongot.index.path.string.StringPath;
import com.xgen.mongot.index.query.scores.Score;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import java.util.List;
import org.bson.BsonValue;

/**
 * SpanTermOperatorDefinition.
 *
 * @param term child term
 */
public record SpanTermOperator(Score score, TermOperator termOperator) implements SpanOperator {
  public static SpanTermOperator fromBson(DocumentParser parser) throws BsonParseException {
    var term = TermOperator.fromBson(parser);
    return new SpanTermOperator(term.score(), term);
  }

  @Override
  public BsonValue spanOperatorToBson() {
    return this.termOperator.operatorToBson();
  }

  @Override
  public List<StringPath> getPaths() {
    return this.termOperator.paths();
  }

  @Override
  public Type getType() {
    return Type.SPAN_TERM;
  }
}
