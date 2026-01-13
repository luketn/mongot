package com.xgen.mongot.server.message;

import static org.junit.Assert.assertEquals;

import com.xgen.mongot.util.BsonUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.Optional;
import org.junit.Test;

public class QueryMessageTest {

  @Test
  public void fromBytes() {
    var header = new MessageHeader(22, 11, 12, OpCode.QUERY);
    ByteBuf buffer = Unpooled.buffer(20);
    buffer.writeBytes(
        new byte[] {15, 0, 0, 0, 'T', 'e', 's', 't', 0, 1, 0, 0, 0, 2, 0, 0, 0, 5, 0, 0, 0, 0});
    QueryMessage parsed = QueryMessage.fromBytes(header, buffer);
    QueryMessage expected =
        new QueryMessage(header, 15, "Test", 1, 2, BsonUtils.emptyDocument(), Optional.empty());

    assertEquals(expected, parsed);
    assertEquals(0, buffer.readableBytes());
  }
}
