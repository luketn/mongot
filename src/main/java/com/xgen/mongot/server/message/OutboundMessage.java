package com.xgen.mongot.server.message;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

public interface OutboundMessage {

  /* Returns the message header */
  MessageHeader getHeader();

  /* Allocates a new ByteBuf and writes the message to it */
  ByteBuf toByteBuf(ByteBufAllocator allocator);
}
