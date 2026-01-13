package com.xgen.testing.mongot.integration.index.serialization;

import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.bson.BsonDocument;
import org.bson.BsonInt32;

public class MqlFilterTestPipelineResult implements DocumentEncodable {
  static class Fields {
    static final Field.Optional<List<BsonDocument>> RESULTS =
        Field.builder("results").documentField().asList().optional().noDefault();

    static final Field.Optional<String> ERROR_MESSAGE_CONTAINS =
        Field.builder("errorMessageContains").stringField().optional().noDefault();
  }

  private final Optional<List<BsonDocument>> results;
  private final Optional<String> errorMessageContains;

  MqlFilterTestPipelineResult(
      Optional<List<BsonDocument>> results, Optional<String> errorMessageContains) {
    this.results = results;
    this.errorMessageContains = errorMessageContains;
  }

  public static MqlFilterTestPipelineResult fromBson(DocumentParser parser)
      throws BsonParseException {
    var results = parser.getField(Fields.RESULTS);
    var errorMessageContains = parser.getField(Fields.ERROR_MESSAGE_CONTAINS);
    parser.getGroup().exactlyOneOf(results, errorMessageContains);
    return new MqlFilterTestPipelineResult(results.unwrap(), errorMessageContains.unwrap());
  }

  @Override
  public BsonDocument toBson() {
    BsonDocumentBuilder builder = BsonDocumentBuilder.builder();
    return builder
        .field(Fields.RESULTS, this.results)
        .field(Fields.ERROR_MESSAGE_CONTAINS, this.errorMessageContains)
        .build();
  }

  public boolean isValid() {
    return this.results.isPresent();
  }

  public String getErrorMessageContains() {
    return Check.isPresent(this.errorMessageContains, "errorMessageContains");
  }

  public Set<Integer> getResultIds() {
    Check.isPresent(this.results, "results");
    return this.results.stream()
        .flatMap(Collection::stream)
        .map(doc -> doc.getInt32("_id"))
        .map(BsonInt32::getValue)
        .collect(Collectors.toSet());
  }
}
