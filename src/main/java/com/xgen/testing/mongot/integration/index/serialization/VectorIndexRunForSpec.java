package com.xgen.testing.mongot.integration.index.serialization;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import org.bson.BsonDocument;

public class VectorIndexRunForSpec implements DocumentEncodable {

  static final VectorIndexRunForSpec DEFAULT =
      new VectorIndexRunForSpec(
          VectorIndexFeatureVersionSpec.DEFAULT, IndexFormatVersionSpec.DEFAULT);

  static class Fields {
    // indexFeatureVersion
    static final Field.WithDefault<VectorIndexFeatureVersionSpec> INDEX_FEATURE_VERSION =
        Field.builder("indexFeatureVersion")
            .classField(VectorIndexFeatureVersionSpec::fromBson)
            .disallowUnknownFields()
            .optional()
            .withDefault(VectorIndexFeatureVersionSpec.DEFAULT);

    // indexFormatVersion
    static final Field.WithDefault<IndexFormatVersionSpec> INDEX_FORMAT_VERSION =
        Field.builder("indexFormatVersion")
            .classField(IndexFormatVersionSpec::fromBson)
            .disallowUnknownFields()
            .optional()
            .withDefault(IndexFormatVersionSpec.DEFAULT);
  }

  private final VectorIndexFeatureVersionSpec indexFeatureVersion;
  private final IndexFormatVersionSpec indexFormatVersion;

  public VectorIndexRunForSpec(
      VectorIndexFeatureVersionSpec indexFeatureVersion,
      IndexFormatVersionSpec indexFormatVersion) {
    this.indexFeatureVersion = indexFeatureVersion;
    this.indexFormatVersion = indexFormatVersion;
  }

  public VectorIndexFeatureVersionSpec getIndexFeatureVersion() {
    return this.indexFeatureVersion;
  }

  public IndexFormatVersionSpec getIndexFormatVersion() {
    return this.indexFormatVersion;
  }

  public static VectorIndexRunForSpec fromBson(DocumentParser parser) throws BsonParseException {
    return new VectorIndexRunForSpec(
        parser.getField(Fields.INDEX_FEATURE_VERSION).unwrap(),
        parser.getField(Fields.INDEX_FORMAT_VERSION).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    var builder = BsonDocumentBuilder.builder();
    builder.field(Fields.INDEX_FEATURE_VERSION, this.indexFeatureVersion);
    builder.field(Fields.INDEX_FORMAT_VERSION, this.indexFormatVersion);
    return builder.build();
  }
}
