package com.xgen.testing.mongot.integration.index.serialization;

import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import org.bson.BsonDocument;

public class IndexFormatVersionSpec implements DocumentEncodable {

  static final IndexFormatVersionSpec DEFAULT =
      new IndexFormatVersionSpec(
          IndexFormatVersion.MIN_SUPPORTED_VERSION.versionNumber,
          IndexFormatVersion.CURRENT.versionNumber);

  static class Fields {

    /**
     * The minimum value of index format version that will be used in testing. The integration/e2e
     * test case will be run for all index format versions starting from the min index format
     * version value up to and including the max index format version value.
     */
    static final Field.WithDefault<Integer> FROM =
        Field.builder("from")
            .intField()
            .mustBeWithinBounds(
                org.apache.commons.lang3.Range.of(
                    IndexFormatVersion.MIN_SUPPORTED_VERSION.versionNumber,
                    IndexFormatVersion.MAX_SUPPORTED_VERSION.versionNumber))
            .optional()
            .withDefault(IndexFormatVersion.MIN_SUPPORTED_VERSION.versionNumber);

    /**
     * The maximum value of index format version that will be used in testing. The integration/e2e
     * test case will be run for all index format versions starting from the min index format
     * version value up to and including the max index format version value.
     */
    static final Field.WithDefault<Integer> TO =
        Field.builder("to")
            .intField()
            .mustBeWithinBounds(
                org.apache.commons.lang3.Range.of(
                    IndexFormatVersion.MIN_SUPPORTED_VERSION.versionNumber,
                    IndexFormatVersion.MAX_SUPPORTED_VERSION.versionNumber))
            .optional()
            .withDefault(IndexFormatVersion.CURRENT.versionNumber);
  }

  private final int from;
  private final int to;

  public IndexFormatVersionSpec(int from, int to) {
    this.from = from;
    this.to = to;
  }

  public int getFrom() {
    return this.from;
  }

  public int getTo() {
    return this.to;
  }

  public static IndexFormatVersionSpec fromBson(DocumentParser parser) throws BsonParseException {
    return new IndexFormatVersionSpec(
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
