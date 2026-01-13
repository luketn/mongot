package com.xgen.mongot.replication.mongodb.initialsync;

import com.xgen.mongot.util.FileUtils;
import com.xgen.mongot.util.bson.JsonCodec;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.Field;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;

public record InitialSyncMetricsMetadata(Optional<Long> startTime) {

  static class Fields {
    public static final Field.Optional<BsonDateTime> START_TIME =
        Field.builder("startTime").bsonDateTimeField().optional().noDefault();
  }

  /** Retrieves metadata from disk if present. */
  public static Optional<InitialSyncMetricsMetadata> fromFileIfExists(Path path)
      throws IOException, BsonParseException {
    File file = path.toFile();
    if (!file.exists()) {
      return Optional.empty();
    }

    String json = Files.readString(path);
    BsonDocument document = JsonCodec.fromJson(json);

    try (BsonDocumentParser parser =
        BsonDocumentParser.fromRoot(document).allowUnknownFields(true).build()) {
      return Optional.of(InitialSyncMetricsMetadata.fromBson(parser));
    }
  }

  public static InitialSyncMetricsMetadata fromBson(BsonDocumentParser parser)
      throws BsonParseException {
    return new InitialSyncMetricsMetadata(
        parser.getField(Fields.START_TIME).unwrap().map(BsonDateTime::getValue));
  }

  /** Persists the metadata atomically. */
  public void persist(Path filePath) throws IOException {
    String json = JsonCodec.toJson(toBson());

    FileUtils.atomicallyReplace(filePath, json);
  }

  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.START_TIME, this.startTime.map(BsonDateTime::new))
        .build();
  }
}
