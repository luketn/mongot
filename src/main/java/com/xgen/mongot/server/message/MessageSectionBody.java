package com.xgen.mongot.server.message;

import com.google.common.base.Objects;
import com.xgen.mongot.util.BsonUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDocument;
import org.bson.RawBsonDocument;
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.io.BasicOutputBuffer;

public final class MessageSectionBody implements MessageSection {
  // We use this registry here as it knows how to encode RawBsonDocument optimally (by copying
  // bytes), and not by treating it as it was a BsonDocument, decoding it into java
  // objects.
  private static final CodecRegistry CODEC_REGISTRY =
      CodecRegistries.fromProviders(new BsonValueCodecProvider());

  private final int size;
  public final RawBsonDocument body;

  public MessageSectionBody(BsonDocument body) {
    this(new RawBsonDocument(body, CODEC_REGISTRY.get(BsonDocument.class)));
  }

  public MessageSectionBody(RawBsonDocument body) {
    this.body = body;
    this.size = body.getByteBuffer().remaining() + 1;
  }

  @Override
  public int size() {
    return this.size;
  }

  @Override
  public ByteBuf toByteBuf(ByteBufAllocator allocator) {
    ByteBuf out = allocator.buffer(this.size);

    out.writeByte(0x00);
    BasicOutputBuffer buffer = new BasicOutputBuffer();
    Codec<RawBsonDocument> codec = CODEC_REGISTRY.get(RawBsonDocument.class);

    try (BsonBinaryWriter writer = new BsonBinaryWriter(buffer)) {
      codec.encode(writer, this.body, BsonUtils.DEFAULT_FAST_CONTEXT);
    }
    out.writeBytes(buffer.getInternalBuffer(), 0, buffer.getSize());

    return out;
  }

  public static MessageSectionBody fromBytes(ByteBuf body) {
    RawBsonDocument doc = MessageUtils.rawBsonDocumentFromBytes(body);
    return new MessageSectionBody(doc);
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof MessageSectionBody that)) {
      return false;
    }
    return this.size == that.size && Objects.equal(this.body, that.body);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(this.size, this.body);
  }
}
