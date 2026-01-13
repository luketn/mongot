package com.xgen.mongot.server.message;

import static com.google.common.truth.Truth.assertThat;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.bson.RawBsonDocument;
import org.junit.Test;

public class ReplyMessageTest {

  // Use parse() because it will allocate a buffer much larger than the actual document
  private static final RawBsonDocument OVERALLOCATED_DOCUMENT = RawBsonDocument.parse("{}");

  @Test
  public void singleton() {
    MessageHeader header = new MessageHeader(100, 3, 8, OpCode.QUERY);
    var msg = new ReplyMessage(header, 7, 123L, 0, OVERALLOCATED_DOCUMENT);

    ByteBuf buf = msg.toByteBuf(UnpooledByteBufAllocator.DEFAULT);
    byte[] result = ByteBufUtil.getBytes(buf);

    assertThat(result)
        .isEqualTo(
            new byte[] {
              41, 0, 0, 0, 17, 0, 0, 0, 3, 0, 0, 0, 1, 0, 0, 0, 7, 0, 0, 0, 123, 0, 0, 0, 0, 0, 0,
              0, 0, 0, 0, 0, 1, 0, 0, 0, 5, 0, 0, 0, 0
            });
  }

  @Test
  public void multi() {
    MessageHeader header = new MessageHeader(100, 3, 8, OpCode.QUERY);
    var msg = new ReplyMessage(header, 7, 123L, 0, OVERALLOCATED_DOCUMENT, OVERALLOCATED_DOCUMENT);

    ByteBuf buf = msg.toByteBuf(UnpooledByteBufAllocator.DEFAULT);
    byte[] result = ByteBufUtil.getBytes(buf);

    assertThat(result)
        .isEqualTo(
            new byte[] {
              46, 0, 0, 0, 17, 0, 0, 0, 3, 0, 0, 0, 1, 0, 0, 0, 7, 0, 0, 0, 123, 0, 0, 0, 0, 0, 0,
              0, 0, 0, 0, 0, 2, 0, 0, 0, 5, 0, 0, 0, 0, 5, 0, 0, 0, 0
            });
  }
}
