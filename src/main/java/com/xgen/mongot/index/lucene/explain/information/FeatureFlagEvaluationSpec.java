package com.xgen.mongot.index.lucene.explain.information;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Encodable;
import com.xgen.mongot.util.bson.parser.Field;
import org.bson.BsonValue;

public record FeatureFlagEvaluationSpec(
    String featureFlagName, boolean evaluationResult, DecisiveField decisiveField)
    implements Encodable {

  static class Fields {
    static final Field.Required<String> FEATURE_FLAG =
        Field.builder("featureFlag").stringField().required();
    static final Field.Required<Boolean> EVALUATION_RESULT =
        Field.builder("evaluationResult").booleanField().required();
    static final Field.Required<DecisiveField> DECISIVE_FIELD =
        Field.builder("decisiveField").enumField(DecisiveField.class).asUpperCamelCase().required();
  }

  public enum DecisiveField {
    PHASE,
    ALLOW_LIST,
    BLOCK_LIST,
    ROLLOUT_PERCENTAGE,
    FALLBACK
  }

  @Override
  public BsonValue toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.FEATURE_FLAG, this.featureFlagName)
        .field(Fields.EVALUATION_RESULT, this.evaluationResult)
        .field(Fields.DECISIVE_FIELD, this.decisiveField)
        .build();
  }

  public static FeatureFlagEvaluationSpec fromBson(DocumentParser parser)
      throws BsonParseException {
    return new FeatureFlagEvaluationSpec(
        parser.getField(Fields.FEATURE_FLAG).unwrap(),
        parser.getField(Fields.EVALUATION_RESULT).unwrap(),
        parser.getField(Fields.DECISIVE_FIELD).unwrap());
  }
}
