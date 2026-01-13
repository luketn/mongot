package com.xgen.testing;

import java.util.Arrays;
import org.bson.ByteBuf;
import org.bson.RawBsonDocument;
import org.intellij.lang.annotations.Language;

/** Utility class for constructing bson values for tests. */
public class BsonTestUtils {

  /**
   * Wrapper around RawBsonDocument.parse(str) where intellij will provide syntax highlighting on
   * the provided json.
   */
  public static RawBsonDocument bson(@Language("json5") String json) {
    return RawBsonDocument.parse(json);
  }

  /** Parses the provided json String as a BsonDocument and returns a minimally-sized byte array. */
  public static byte[] bsonBytes(@Language("json5") String json) {
    ByteBuf buf = bson(json).getByteBuffer();
    return Arrays.copyOfRange(buf.array(), buf.position(), buf.position() + buf.remaining());
  }
}
