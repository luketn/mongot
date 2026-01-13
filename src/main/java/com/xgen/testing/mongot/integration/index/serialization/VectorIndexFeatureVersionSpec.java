package com.xgen.testing.mongot.integration.index.serialization;

import com.xgen.mongot.index.definition.VectorIndexCapabilities;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import org.bson.BsonDocument;

public class VectorIndexFeatureVersionSpec implements DocumentEncodable {

  static final VectorIndexFeatureVersionSpec DEFAULT =
      new VectorIndexFeatureVersionSpec(
          VectorIndexCapabilities.MIN_SUPPORTED_FEATURE_VERSION,
          VectorIndexCapabilities.CURRENT_FEATURE_VERSION);

  static class Fields {

    /**
     * The minimum value of index feature version that will be used in testing. The integration/e2e
     * test case will be run for all index feature versions starting from the min index feature
     * version value up to and including the max index feature version value.
     */
    static final Field.WithDefault<Integer> FROM =
        Field.builder("from")
            .intField()
            .mustBeWithinBounds(
                org.apache.commons.lang3.Range.of(
                    VectorIndexCapabilities.MIN_SUPPORTED_FEATURE_VERSION,
                    VectorIndexCapabilities.CURRENT_FEATURE_VERSION))
            .optional()
            .withDefault(VectorIndexCapabilities.CURRENT_FEATURE_VERSION);

    /**
     * The maximum value of index feature version that will be used in testing. The integration/e2e
     * test case will be run for all index feature versions starting from the min index feature
     * version value up to and including the max index feature version value.
     */
    static final Field.WithDefault<Integer> TO =
        Field.builder("to")
            .intField()
            .mustBeWithinBounds(
                org.apache.commons.lang3.Range.of(
                    VectorIndexCapabilities.MIN_SUPPORTED_FEATURE_VERSION,
                    VectorIndexCapabilities.CURRENT_FEATURE_VERSION))
            .optional()
            .withDefault(VectorIndexCapabilities.CURRENT_FEATURE_VERSION);
  }

  private final int from;
  private final int to;

  public VectorIndexFeatureVersionSpec(int from, int to) {
    this.from = from;
    this.to = to;
  }

  public int getFrom() {
    return this.from;
  }

  public int getTo() {
    return this.to;
  }

  public static VectorIndexFeatureVersionSpec fromBson(DocumentParser parser)
      throws BsonParseException {
    return new VectorIndexFeatureVersionSpec(
        parser.getField(Fields.FROM).unwrap(), parser.getField(Fields.TO).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    var builder = BsonDocumentBuilder.builder();
    builder.field(Fields.FROM, this.from);
    builder.field(Fields.TO, this.to);
    return builder.build();
  }
}
