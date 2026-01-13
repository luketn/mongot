package com.xgen.testing.mongot.integration.index.serialization;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.List;
import org.bson.BsonDocument;

/** Results expected to be with same/similar score and with no ordering guarantee */
public class ExpectedResultGroup implements ExpectedResult {

  private static class Fields {

    private static final Field.Required<List<ExpectedResultItem>> UNORDERED_RESULTS =
        Field.builder("unorderedResults")
            .classField(ExpectedResultItem::fromBson)
            .disallowUnknownFields()
            .asList()
            .required();
  }

  private final List<ExpectedResultItem> unorderedResults;

  public ExpectedResultGroup(List<ExpectedResultItem> unorderedResults) {
    this.unorderedResults = unorderedResults;
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.UNORDERED_RESULTS, this.unorderedResults)
        .build();
  }

  static ExpectedResultGroup fromBson(DocumentParser parser) throws BsonParseException {
    return new ExpectedResultGroup(parser.getField(Fields.UNORDERED_RESULTS).unwrap());
  }

  @Override
  public List<ExpectedResultItem> getResults() {
    return this.unorderedResults;
  }

  @Override
  public Type getType() {
    return Type.GROUP;
  }
}
