package com.xgen.mongot.util.bson;

import com.xgen.mongot.util.BsonUtils;
import java.util.Arrays;
import org.apache.lucene.util.BytesRef;
import org.bson.BsonDocument;
import org.bson.ByteBuf;
import org.bson.RawBsonDocument;

public class ByteUtils {
  public static byte[] toByteArray(BsonDocument doc) {
    var buffer = new RawBsonDocument(doc, BsonUtils.BSON_DOCUMENT_CODEC).getByteBuffer();
    return Arrays.copyOf(buffer.array(), buffer.remaining());
  }

  /**
   * Wraps the given {@link RawBsonDocument} in a {@link BytesRef}. If the underlying byte[] is
   * changed, the change will be reflected in the returned BytesRef.
   */
  public static BytesRef toBytesRef(RawBsonDocument doc) {
    ByteBuf buf = doc.getByteBuffer();
    return new BytesRef(buf.array(), buf.position(), buf.remaining());
  }

  /**
   * Creates a {@link RawBsonDocument} that wraps a copy of the given {@link BytesRef} data.
   *
   * <p>The resulting RawBsonDocument is immutable even if the original BytesRef changes.
   */
  public static RawBsonDocument fromBytesRef(BytesRef bytes) {
    // Note: In general, this needs to be a defensive copy because BytesRef can be reused by Lucene.
    byte[] copy = Arrays.copyOfRange(bytes.bytes, bytes.offset, bytes.length);
    return new RawBsonDocument(copy);
  }
}
