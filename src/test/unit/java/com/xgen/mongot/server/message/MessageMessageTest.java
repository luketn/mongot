package com.xgen.mongot.server.message;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.UnpooledByteBufAllocator;
import java.util.List;
import org.bson.BsonDocument;
import org.bson.RawBsonDocument;
import org.junit.Test;

public class MessageMessageTest {

  // Use parse() because it will allocate a buffer much larger than the actual document
  private static final RawBsonDocument OVERALLOCATED_DOCUMENT = RawBsonDocument.parse("{}");

  @Test
  public void documentArrayOversized() {
    var buf = OVERALLOCATED_DOCUMENT.getByteBuffer();

    assertThat(buf.array().length).isGreaterThan(buf.remaining());
  }

  @Test
  public void forResponse() {
    List<MessageSection> section = List.of(new MessageSectionBody(OVERALLOCATED_DOCUMENT));
    MessageMessage msg = MessageMessage.forResponse(1870, 7, section);
    ByteBuf buf = msg.toByteBuf(UnpooledByteBufAllocator.DEFAULT);

    byte[] result = ByteBufUtil.getBytes(buf);

    assertThat(result)
        .isEqualTo(
            new byte[] {
              26, 0, 0, 0, 17, 0, 0, 0, 78, 7, 0, 0, -35, 7, 0, 0, 7, 0, 0, 0, 0, 5, 0, 0, 0, 0
            });
    var recovered =
        MessageMessage.fromBytes(msg.getHeader(), buf.readerIndex(MessageHeader.SIZE_IN_BYTES));
    assertEquals(msg, recovered);
    assertEquals(0, buf.readableBytes());
  }

  @Test
  public void forResponseSingletonSequence() {
    List<BsonDocument> docs = List.of(OVERALLOCATED_DOCUMENT);

    List<MessageSection> section = List.of(new MessageSectionDocumentSequence("admin.$cmd", docs));
    MessageMessage msg = MessageMessage.forResponse(1870, 7, section);
    ByteBuf buf = msg.toByteBuf(UnpooledByteBufAllocator.DEFAULT);

    byte[] result = ByteBufUtil.getBytes(buf);

    assertThat(result)
        .isEqualTo(
            new byte[] {
              37, 0, 0, 0, 17, 0, 0, 0, 78, 7, 0, 0, -35, 7, 0, 0, 7, 0, 0, 0, 1, 20, 0, 0, 0, 97,
              100, 109, 105, 110, 46, 36, 99, 109, 100, 0, 5, 0, 0, 0, 0
            });
    var recovered =
        MessageMessage.fromBytes(msg.getHeader(), buf.readerIndex(MessageHeader.SIZE_IN_BYTES));
    assertEquals(msg, recovered);
    assertEquals(0, buf.readableBytes());
  }

  @Test
  public void forResponseSequence() {
    List<BsonDocument> docs = List.of(OVERALLOCATED_DOCUMENT, OVERALLOCATED_DOCUMENT);

    List<MessageSection> section = List.of(new MessageSectionDocumentSequence("admin.$cmd", docs));
    MessageMessage msg = MessageMessage.forResponse(1870, 7, section);
    ByteBuf buf = msg.toByteBuf(UnpooledByteBufAllocator.DEFAULT);

    byte[] result = ByteBufUtil.getBytes(buf);

    assertThat(result)
        .isEqualTo(
            new byte[] {
              42, 0, 0, 0, 17, 0, 0, 0, 78, 7, 0, 0, -35, 7, 0, 0, 7, 0, 0, 0, 1, 25, 0, 0, 0, 97,
              100, 109, 105, 110, 46, 36, 99, 109, 100, 0, 5, 0, 0, 0, 0, 5, 0, 0, 0, 0
            });
    var recovered =
        MessageMessage.fromBytes(msg.getHeader(), buf.readerIndex(MessageHeader.SIZE_IN_BYTES));
    assertEquals(msg, recovered);
    assertEquals(0, buf.readableBytes());
  }
}
