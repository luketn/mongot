package com.xgen.mongot.index.query.operators;

import com.xgen.mongot.index.query.scores.Score;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.Value;
import java.util.List;
import org.bson.BsonDocument;
import org.bson.BsonValue;

public record MoreLikeThisOperator(List<BsonDocument> like, Score score) implements Operator {
  public static class Fields {
    public static final Field.Required<List<BsonDocument>> LIKE =
        Field.builder("like")
            .singleValueOrListOf(Value.builder().documentValue().required())
            .mustNotBeEmpty()
            .required();
  }

  @Override
  public Type getType() {
    return Type.MORE_LIKE_THIS;
  }

  @Override
  public BsonValue operatorToBson() {
    return Operators.documentBuilder(score()).field(Fields.LIKE, this.like).build();
  }

  public static MoreLikeThisOperator fromBson(DocumentParser parser) throws BsonParseException {
    return new MoreLikeThisOperator(
        parser.getField(Fields.LIKE).unwrap(), Operators.parseScore(parser));
  }
}
