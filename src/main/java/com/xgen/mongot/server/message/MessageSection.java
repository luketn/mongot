package com.xgen.mongot.server.message;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

public sealed interface MessageSection permits MessageSectionBody, MessageSectionDocumentSequence {

  int size();

  ByteBuf toByteBuf(ByteBufAllocator allocator);

  static MessageSection fromBytes(ByteBuf body) {

    int kind = body.readByte();
    return switch (kind) {
      case 0 -> MessageSectionBody.fromBytes(body);
      case 1 -> MessageSectionDocumentSequence.fromBytes(body);
      default -> throw new IllegalArgumentException("unknown message section kind " + kind);
    };
  }
}
