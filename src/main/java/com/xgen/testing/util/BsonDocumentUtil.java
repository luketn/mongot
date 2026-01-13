package com.xgen.testing.util;

import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.Bytes;
import com.xgen.mongot.util.bson.BsonArrayBuilder;
import java.util.function.Supplier;
import org.bson.BsonArray;
import org.bson.BsonBinaryReader;
import org.bson.BsonDocument;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.ByteBuf;
import org.bson.RawBsonDocument;
import org.bson.codecs.BsonValueCodec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.RawBsonDocumentCodec;
import org.bson.io.ByteBufferBsonInput;

public class BsonDocumentUtil {

  private BsonDocumentUtil() {}

  /**
   * Decodes a (possibly invalid) {@link RawBsonDocument} into a human-readable string for
   * debugging. This method keeps going even after encountering invalid bson. If it's not possible
   * to fully decode the input, this method returns the prefix of what was decoded plus the error
   * message.
   */
  public static String decodeCorruptDocument(RawBsonDocument doc) {
    StringBuilder b = new StringBuilder("RawBsonDocument(");
    try {
      ByteBuf buffer = doc.getByteBuffer();
      BsonBinaryReader reader = new BsonBinaryReader(new ByteBufferBsonInput(buffer));
      reader.readStartDocument();
      int written = doc.getByteBuffer().getInt();
      int actual = doc.getByteBuffer().remaining();
      if (written == actual) {
        b.append("length=").append(actual);
      } else {
        b.append("length=").append(actual).append(" but wrote ").append(written).append("!");
      }
      b.append(": ");

      while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
        b.append(reader.getCurrentBsonType());
        b.append(" '");
        b.append(reader.readName());
        b.append("'=");

        if (reader.getCurrentBsonType() == BsonType.DOCUMENT) {
          var mark = reader.getMark();
          try {
            var inner = new RawBsonDocumentCodec().decode(reader, DecoderContext.builder().build());
            b.append(decodeCorruptDocument(inner));
          } catch (RuntimeException e) {
            mark.reset();
            int length = reader.getBsonInput().readInt32();
            mark.reset();
            b.append("length=").append(length).append(": ");
            reader.readStartDocument();
            while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
              b.append(reader.getCurrentBsonType());
              b.append(' ');
              b.append(reader.readName());
              b.append('=');
              var value = new BsonValueCodec().decode(reader, DecoderContext.builder().build());
              b.append(value);
            }
            throw e;
          }
        } else {
          var value = new BsonValueCodec().decode(reader, DecoderContext.builder().build());
          b.append(value);
        }
        b.append(", ");
      }
      return b.append(")").toString();
    } catch (RuntimeException e) {
      return b.append("-- ").append(e.getMessage()).toString();
    }
  }

  @SuppressWarnings("checkstyle:MissingJavadocMethod")
  public static BsonDocument createDocument(Bytes sizeLimit, Supplier<BsonValue> contentSupplier) {

    var document = new BsonDocument().append("array", new BsonArray());

    // calculate size limit based on this document overhead
    var arrayLimit =
        sizeLimit.subtract(
            Bytes.ofBytes(
                new RawBsonDocument(document, BsonUtils.BSON_DOCUMENT_CODEC)
                    .getByteBuffer()
                    .remaining()));

    var builder = BsonArrayBuilder.withLimit(arrayLimit);

    while (true) {
      if (!builder.append(
          new RawBsonDocument(
              new BsonDocument("key", contentSupplier.get()), BsonUtils.BSON_DOCUMENT_CODEC))) {
        break;
      }
    }

    document.put("array", builder.build());

    return document;
  }

  public static Integer getId(BsonDocument bsonDocument) {
    return bsonDocument.get("_id").asInt32().getValue();
  }
}
