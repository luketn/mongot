package com.xgen.mongot.server.message;

import com.google.common.base.Objects;
import com.google.errorprone.annotations.Var;
import com.xgen.mongot.util.BsonUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDocument;
import org.bson.RawBsonDocument;
import org.bson.io.BasicOutputBuffer;

public final class MessageSectionDocumentSequence implements MessageSection {

  private final int size;
  private final String id;
  private final List<BsonDocument> objects;

  public MessageSectionDocumentSequence(String id, List<BsonDocument> objects) {
    this.id = id;
    this.objects = objects;

    @Var int size = 1 + id.length() + 1;
    for (BsonDocument object : objects) {
      RawBsonDocument rawDoc = new RawBsonDocument(object, BsonUtils.BSON_DOCUMENT_CODEC);
      size += rawDoc.getByteBuffer().remaining();
    }
    this.size = size;
  }

  @Override
  public int size() {
    return this.size;
  }

  @Override
  public ByteBuf toByteBuf(ByteBufAllocator allocator) {
    @Var int size = 4 + this.id.length() + 1;
    List<BasicOutputBuffer> objectBuffers = new ArrayList<>(this.objects.size());
    for (BsonDocument object : this.objects) {
      BasicOutputBuffer buffer = new BasicOutputBuffer();
      objectBuffers.add(buffer);
      try (BsonBinaryWriter writer = new BsonBinaryWriter(buffer)) {
        BsonUtils.BSON_DOCUMENT_CODEC.encode(writer, object, BsonUtils.DEFAULT_FAST_CONTEXT);
      }
      size += buffer.getSize();
    }

    ByteBuf out = allocator.buffer(size + 1);

    out.writeByte(0x01);
    out.writeIntLE(size);
    out.writeCharSequence(this.id, StandardCharsets.UTF_8);
    out.writeByte(0x00);

    for (BasicOutputBuffer buf : objectBuffers) {
      out.writeBytes(buf.getInternalBuffer(), 0, buf.getSize());
    }

    return out;
  }

  static MessageSectionDocumentSequence fromBytes(ByteBuf body) {
    int size = body.readIntLE();
    String id = MessageUtils.readCString(body);

    @Var int sizeLeft = size - 4 - id.length() - 1;

    List<BsonDocument> objects = new ArrayList<>();
    while (sizeLeft > 0) {
      RawBsonDocument doc = MessageUtils.rawBsonDocumentFromBytes(body);
      objects.add(doc);
      sizeLeft -= doc.getByteBuffer().remaining();
    }

    return new MessageSectionDocumentSequence(id, objects);
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof MessageSectionDocumentSequence that)) {
      return false;
    }
    return this.size == that.size
        && Objects.equal(this.id, that.id)
        && Objects.equal(this.objects, that.objects);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(this.size, this.id, this.objects);
  }
}
