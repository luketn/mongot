package com.xgen.mongot.util.bson;

import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.Value;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.commons.io.FilenameUtils;
import org.bson.BsonArray;
import org.bson.BsonBinaryReader;
import org.bson.BsonDocument;
import org.bson.BsonInvalidOperationException;
import org.bson.BsonSerializationException;
import org.bson.codecs.DecoderContext;

public class DocumentUtil {

  private static final Value.Required<List<BsonDocument>> DOCUMENT_ARRAY_VALUE =
      Value.builder().documentValue().asList().required();

  /**
   * Read in a file (.json or .bson) and convert to BsonDocument.
   *
   * <p>Throws IOException, BsonParseException if issues parsing json/bson.
   */
  public static BsonDocument documentFromPath(Path path) throws IOException, BsonParseException {
    String ext = FilenameUtils.getExtension(path.toString());

    if (ext.equals("bson")) {
      byte[] data = Files.readAllBytes(path);
      ByteBuffer buffer = ByteBuffer.wrap(data);

      try (BsonBinaryReader bsonBinaryReader = new BsonBinaryReader(buffer)) {
        try {
          return BsonUtils.BSON_DOCUMENT_CODEC.decode(
              bsonBinaryReader, DecoderContext.builder().build());
        } catch (BsonSerializationException exception) {
          throw new BsonParseException(exception);
        }
      }
    } else if (ext.equals("json")) {
      return JsonCodec.fromJson(Files.readString(path));
    } else {
      throw new IOException("Unsupported file type: " + ext);
    }
  }

  /**
   * Read in a file (json or bson) that contains an array with multiple documents in it.
   *
   * <p>Throws IOException, BsonParseException if issues parsing json/bson.
   */
  public static List<BsonDocument> documentsFromPath(Path path)
      throws IOException, BsonParseException {
    String ext = FilenameUtils.getExtension(path.toString());

    if (ext.equals("bson")) {
      try (BsonBinaryReader bsonBinaryReader =
          new BsonBinaryReader(ByteBuffer.wrap(Files.readAllBytes(path)))) {

        try {
          return DOCUMENT_ARRAY_VALUE
              .getParser()
              .parse(
                  BsonParseContext.root(),
                  BsonUtils.BSON_ARRAY_CODEC.decode(
                      bsonBinaryReader, DecoderContext.builder().build()));
        } catch (BsonInvalidOperationException exception) {
          throw new BsonParseException(exception);
        }
      }
    } else if (ext.equals("json")) {
      return DOCUMENT_ARRAY_VALUE
          .getParser()
          .parse(BsonParseContext.root(), BsonArray.parse(Files.readString(path)));
    } else {
      throw new IOException("Unsupported file type: " + ext);
    }
  }

  /**
   * Only parses input structured as a json for now.
   *
   * <p>Assumes that system input is in UTF-8 format.
   *
   * <p>Requires document(s) to be in an array (even if single document)
   */
  public static List<BsonDocument> parseStream(InputStream stream)
      throws BsonParseException, IOException {
    byte[] inputBytes = stream.readAllBytes();

    String text = new String(inputBytes, StandardCharsets.UTF_8);

    return DOCUMENT_ARRAY_VALUE.getParser().parse(BsonParseContext.root(), BsonArray.parse(text));
  }
}
