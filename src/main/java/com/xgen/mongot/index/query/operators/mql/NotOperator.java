package com.xgen.mongot.index.query.operators.mql;

import static com.xgen.mongot.util.Check.checkArg;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonValue;

public record NotOperator(MqlFilterOperatorList negateValues) implements MqlFilterOperator {

  static class Fields {
    static final Field.Optional<NotOperator> NOT =
        Field.builder("$not")
            .classField(NotOperator::fromBson, NotOperator::operatorToBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();
  }

  public NotOperator {
    checkArg(!negateValues.mqlFilterOperators().isEmpty(), "NotOperator needs a value to negate");
  }

  public static NotOperator fromBson(DocumentParser parser) throws BsonParseException {
    return new NotOperator(MqlFilterOperatorList.fromBson(parser));
  }

  public BsonValue operatorToBson() {
    return this.negateValues.toBson();
  }

  @Override
  public Category getCategory() {
    return Category.NOT;
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder().field(Fields.NOT, Optional.of(this)).build();
  }
}
