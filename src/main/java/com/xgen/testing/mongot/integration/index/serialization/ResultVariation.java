package com.xgen.testing.mongot.integration.index.serialization;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.testing.mongot.integration.index.serialization.variations.TestRunVariant;
import java.util.List;
import org.bson.BsonDocument;

/**
 * {@link ResultVariation#expectedResultsOutline} will always get their details from {@link
 * ValidResult#getDefaultSearchResults()} identifying counterparts based on IDs. When no IDs are
 * present in {@link ValidResult#getDefaultSearchResults()}, full details should be specified in
 * variational results.
 */
public class ResultVariation implements DocumentEncodable {

  public static class Fields {

    public static final Field.Required<List<TestRunVariant>> APPLICABLE_VARIANTS =
        Field.builder("applicableVariants")
            .classField(TestRunVariant::fromBson)
            .disallowUnknownFields()
            .asList()
            .mustNotBeEmpty()
            .required();

    public static final Field.Required<List<ExpectedResult>> EXPECTED_RESULTS_OUTLINE =
        Field.builder("expectedResultsOutline")
            .classField(ExpectedResult::fromBson)
            .disallowUnknownFields()
            .asList()
            .required();
  }

  private final List<TestRunVariant> applicableVariants;
  private final List<ExpectedResult> expectedResultsOutline;

  public ResultVariation(
      List<TestRunVariant> applicableVariants, List<ExpectedResult> expectedResultsOutline) {
    this.applicableVariants = applicableVariants;
    this.expectedResultsOutline = expectedResultsOutline;
  }

  static ResultVariation fromBson(DocumentParser parser) throws BsonParseException {
    return new ResultVariation(
        parser.getField(Fields.APPLICABLE_VARIANTS).unwrap(),
        parser.getField(ResultVariation.Fields.EXPECTED_RESULTS_OUTLINE).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(ResultVariation.Fields.APPLICABLE_VARIANTS, this.applicableVariants)
        .field(ResultVariation.Fields.EXPECTED_RESULTS_OUTLINE, this.expectedResultsOutline)
        .build();
  }

  public List<TestRunVariant> getApplicableVariants() {
    return this.applicableVariants;
  }

  public List<ExpectedResult> getExpectedResultsOutline() {
    return this.expectedResultsOutline;
  }
}
