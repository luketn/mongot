package com.xgen.mongot.server.grpc;

import static org.junit.Assert.assertThrows;

import com.xgen.mongot.server.message.MessageMessage;
import com.xgen.mongot.server.message.OpCode;
import io.grpc.StatusRuntimeException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.junit.Assert;
import org.junit.Test;

public class MessageMarshallerTest {
  /** Creates an example MessageMessage and returns the corresponding byte array. */
  private static ByteBuffer getBytesForExampleOpMsg() {
    ByteBuf buf =
        MessageMessage.forResponse(233, 0, new ArrayList<>())
            .getOutboundMessage(new BsonDocument().append("hello", BsonBoolean.TRUE))
            .toByteBuf(ByteBufAllocator.DEFAULT);
    byte[] msgBytes = new byte[buf.readableBytes()];
    buf.readBytes(msgBytes);
    buf.release();
    return ByteBuffer.wrap(msgBytes).order(ByteOrder.LITTLE_ENDIAN);
  }

  @Test
  public void testBasicEncodeAndDecode() throws IOException {
    var buffer = getBytesForExampleOpMsg();
    MessageMarshaller messageMarshaller = new MessageMarshaller();
    MessageMessage message = messageMarshaller.parse(new ByteArrayInputStream(buffer.array()));
    try (InputStream stream = messageMarshaller.stream(message)) {
      Assert.assertArrayEquals(buffer.array(), stream.readAllBytes());
    }
  }

  @Test
  public void testNotEnoughBytes() {
    var buffer = ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN);
    buffer.putInt(12);
    buffer.putInt(0xCAFE);
    buffer.putInt(0xFEED);
    var msgBytes = buffer.flip().array();
    MessageMarshaller messageMarshaller = new MessageMarshaller();
    assertThrows(
        StatusRuntimeException.class,
        () -> messageMarshaller.parse(new ByteArrayInputStream(msgBytes)));
  }

  @Test
  public void testSizeTooSmall() {
    var buffer = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN);
    buffer.putInt(12);
    buffer.putInt(0xCAFE);
    buffer.putInt(0xFEED);
    buffer.putInt(0xFADE);
    var msgBytes = buffer.flip().array();
    MessageMarshaller messageMarshaller = new MessageMarshaller();
    assertThrows(
        StatusRuntimeException.class,
        () -> messageMarshaller.parse(new ByteArrayInputStream(msgBytes)));
  }

  @Test
  public void testInvalidOpCode() {
    var buffer = getBytesForExampleOpMsg();
    buffer.putInt(12, OpCode.QUERY.code);
    MessageMarshaller messageMarshaller = new MessageMarshaller();
    assertThrows(
        StatusRuntimeException.class,
        () -> messageMarshaller.parse(new ByteArrayInputStream(buffer.array())));
  }
}
