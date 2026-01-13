package com.xgen.mongot.server.message;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.util.BsonUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.util.ArrayList;
import java.util.List;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDocument;
import org.bson.io.BasicOutputBuffer;

public class ReplyMessage implements OutboundMessage {

  private final MessageHeader clientHeader;
  private final int flags;
  private final long cursorId;
  private final int startingFrom;
  private final BsonDocument[] documents;

  public ReplyMessage(
      MessageHeader clientHeader,
      int flags,
      long cursorId,
      int startingFrom,
      BsonDocument... documents) {
    this.clientHeader = clientHeader;
    this.flags = flags;
    this.cursorId = cursorId;
    this.startingFrom = startingFrom;
    this.documents = documents;
  }

  @Override
  public ByteBuf toByteBuf(ByteBufAllocator allocator) {
    @Var int size = 36;
    List<BasicOutputBuffer> documentBuffers = new ArrayList<>(this.documents.length);
    for (BsonDocument document : this.documents) {
      BasicOutputBuffer buffer = new BasicOutputBuffer();
      documentBuffers.add(buffer);
      try (BsonBinaryWriter writer = new BsonBinaryWriter(buffer)) {
        BsonUtils.BSON_DOCUMENT_CODEC.encode(writer, document, BsonUtils.DEFAULT_FAST_CONTEXT);
      }
      size += buffer.getSize();
    }

    ByteBuf out = allocator.buffer(size);

    out.writeIntLE(size);
    out.writeIntLE(17);
    out.writeIntLE(this.clientHeader.requestId());
    out.writeIntLE(OpCode.REPLY.code);

    out.writeIntLE(this.flags);
    out.writeLongLE(this.cursorId);
    out.writeIntLE(this.startingFrom);
    out.writeIntLE(this.documents.length);

    for (BasicOutputBuffer buf : documentBuffers) {
      out.writeBytes(buf.getInternalBuffer(), 0, buf.getSize());
    }
    return out;
  }

  @Override
  public MessageHeader getHeader() {
    return this.clientHeader;
  }
}
