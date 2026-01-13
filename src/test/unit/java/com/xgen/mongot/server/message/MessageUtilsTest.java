package com.xgen.mongot.server.message;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import org.bson.BsonDocument;
import org.bson.RawBsonDocument;
import org.junit.Test;

public class MessageUtilsTest {

  @Test
  public void readCString() {
    ByteBuf buffer = Unpooled.buffer(16);
    buffer.writeByte(0); // Ensure we use relative reads and update index properly
    buffer.writeCharSequence("Hello World", StandardCharsets.UTF_8);
    buffer.writeByte(0);
    buffer.readerIndex(1);

    String result = MessageUtils.readCString(buffer);

    assertEquals("Hello World".length(), result.length());
    assertEquals("Hello World", result);
    assertEquals("Hello World".length() + 2, buffer.readerIndex());
  }

  @Test
  public void emptyCString() {
    ByteBuf buffer = Unpooled.buffer(16);
    buffer.writeByte(0);

    String result = MessageUtils.readCString(buffer);

    assertEquals("", result);
    assertEquals(1, buffer.readerIndex());
  }

  @Test
  public void readMissingCString() {
    ByteBuf buffer = Unpooled.buffer(16);
    buffer.writeCharSequence("A".repeat(16), StandardCharsets.UTF_8);

    assertThrows(IndexOutOfBoundsException.class, () -> MessageUtils.readCString(buffer));
  }


  @Test
  public void readRawBsonDocument() {
    RawBsonDocument first = RawBsonDocument.parse("{x: 1}");
    RawBsonDocument second = RawBsonDocument.parse("{x: 2}");
    ByteBuf buffer = Unpooled.buffer(16);
    buffer.writeBytes(first.getByteBuffer().asNIO());
    buffer.writeBytes(second.getByteBuffer().asNIO());

    RawBsonDocument a = MessageUtils.rawBsonDocumentFromBytes(buffer);
    RawBsonDocument b = MessageUtils.rawBsonDocumentFromBytes(buffer);

    assertEquals(first, a);
    assertEquals(second, b);
  }

  @Test
  public void readRawBsonDocumentTooShort() {
    RawBsonDocument first = RawBsonDocument.parse("{x: 1}");
    ByteBuf buffer = Unpooled.buffer(16);
    buffer.writeIntLE(1000);
    buffer.writeBytes(first.getByteBuffer().asNIO());

    assertThrows(
        IndexOutOfBoundsException.class, () -> MessageUtils.rawBsonDocumentFromBytes(buffer));
  }

  @Test
  public void createErrorBodyNullMessage() {
    BsonDocument expected = MessageUtils.createErrorBody("IllegalStateException");

    BsonDocument result = MessageUtils.createErrorBody(new IllegalStateException());

    assertEquals(expected, result);
  }

  @Test
  public void createErrorBodyEquivalentIfNotNull() {
    BsonDocument expected = MessageUtils.createErrorBody("message");

    BsonDocument result = MessageUtils.createErrorBody(new Exception("message"));

    assertEquals(expected, result);
  }
}
