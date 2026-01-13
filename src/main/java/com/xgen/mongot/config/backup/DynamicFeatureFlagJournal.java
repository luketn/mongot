package com.xgen.mongot.config.backup;

import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagConfig;
import com.xgen.mongot.util.bson.JsonCodec;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import org.bson.BsonDocument;

public record DynamicFeatureFlagJournal(List<DynamicFeatureFlagConfig> dynamicFeatureFlags)
    implements DocumentEncodable {

  public static class Fields {
    static final Field.WithDefault<List<DynamicFeatureFlagConfig>> DYNAMIC_FEATURE_FLAGS =
        Field.builder("dynamicFeatureFlags")
            .classField(DynamicFeatureFlagConfig::fromBson)
            .allowUnknownFields()
            .asList()
            .optional()
            .withDefault(List.of());
  }

  public static Optional<DynamicFeatureFlagJournal> fromFileIfExists(Path path)
      throws IOException, BsonParseException {
    File file = path.toFile();
    if (!file.exists()) {
      return Optional.empty();
    }

    String json = Files.readString(path);
    BsonDocument document = JsonCodec.fromJson(json);

    try (BsonDocumentParser parser =
        BsonDocumentParser.fromRoot(document).allowUnknownFields(true).build()) {
      return Optional.of(fromBson(parser));
    }
  }

  public static DynamicFeatureFlagJournal fromBson(DocumentParser parser)
      throws BsonParseException {

    return new DynamicFeatureFlagJournal(parser.getField(Fields.DYNAMIC_FEATURE_FLAGS).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.DYNAMIC_FEATURE_FLAGS, this.dynamicFeatureFlags)
        .build();
  }
}
