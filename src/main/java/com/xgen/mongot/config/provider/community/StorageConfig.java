package com.xgen.mongot.config.provider.community;

import com.xgen.mongot.config.provider.community.parser.PathField;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.nio.file.Path;
import org.bson.BsonDocument;

public record StorageConfig(Path dataPath) implements DocumentEncodable {
  private static class Fields {
    public static final Field.Required<Path> DATA_PATH =
        Field.builder("dataPath").classField(PathField.PARSER, PathField.ENCODER).required();
  }

  public static StorageConfig fromBson(DocumentParser parser) throws BsonParseException {
    return new StorageConfig(parser.getField(Fields.DATA_PATH).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder().field(Fields.DATA_PATH, this.dataPath).build();
  }
}
