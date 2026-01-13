package com.xgen.mongot.util.bson;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.Bytes;
import java.util.UUID;
import org.apache.commons.lang3.RandomStringUtils;
import org.bson.BsonDocument;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.RawBsonDocument;
import org.junit.Assert;
import org.junit.Test;

public class BsonArrayBuilderTest {

  @Test
  public void testBuilderReturnsFalseWhenDocumentDoesNotFit() {

    var element =
        new RawBsonDocument(
            new BsonDocument(
                UUID.randomUUID().toString(), new BsonString(UUID.randomUUID().toString())),
            BsonUtils.BSON_DOCUMENT_CODEC);

    var elementSize = element.getByteBuffer().remaining();
    var builder = BsonArrayBuilder.withLimit(Bytes.ofBytes(Math.round(elementSize * 1.5)));

    Assert.assertTrue(builder.append(element));
    Assert.assertFalse(builder.append(element));

    var array = builder.build();
    Assert.assertEquals(1, array.size());
  }

  @Test
  public void testArraySizeIsLimitedCorrectly() {

    var element =
        new RawBsonDocument(
            new BsonDocument(
                RandomStringUtils.randomAlphabetic(0, 5),
                new BsonString(RandomStringUtils.randomAlphabetic(0, 5))),
            BsonUtils.BSON_DOCUMENT_CODEC);

    var elementSize = element.getByteBuffer().remaining();
    var arraySizeLimit = Bytes.ofBytes(elementSize * 1000L);
    var builder = BsonArrayBuilder.withLimit(arraySizeLimit);
    @Var var elementCount = 0;

    while (builder.append(element)) {
      elementCount++;
    }

    var array = builder.build();

    var wrapperDocumentSizeBytes =
        new RawBsonDocument(new BsonDocument("", new BsonNull()), BsonUtils.BSON_DOCUMENT_CODEC)
            .getByteBuffer()
            .remaining();

    var wrappedArraySizeBytes =
        new RawBsonDocument(new BsonDocument("", array), BsonUtils.BSON_DOCUMENT_CODEC)
            .getByteBuffer()
            .remaining();

    var arraySize = Bytes.ofBytes(wrappedArraySizeBytes - wrapperDocumentSizeBytes);

    Assert.assertEquals(elementCount, array.size());
    // check that result array size is lower than or equal to limit
    Assert.assertTrue(arraySizeLimit.subtract(arraySize).toBytes() >= 0);
  }

  @Test
  public void testDocumentCount() {

    var element =
        new RawBsonDocument(
            new BsonDocument(
                UUID.randomUUID().toString(), new BsonString(UUID.randomUUID().toString())),
            BsonUtils.BSON_DOCUMENT_CODEC);

    var builder = BsonArrayBuilder.withLimit(Bytes.ofMebi(1));

    for (int i = 0; i < 5; i++) {
      builder.append(element);
    }

    Assert.assertEquals(5, builder.getDocumentCount());
  }

  @Test
  public void testDataSize() {

    var element =
        new RawBsonDocument(
            new BsonDocument(
                UUID.randomUUID().toString(), new BsonString(UUID.randomUUID().toString())),
            BsonUtils.BSON_DOCUMENT_CODEC);

    var elementSize = element.getByteBuffer().remaining();
    var builder = BsonArrayBuilder.withLimit(Bytes.ofMebi(1));

    for (int i = 0; i < 5; i++) {
      builder.append(element);
    }

    Assert.assertTrue(builder.getDataSize().toBytes() > elementSize * 5L);
  }

  @Test
  public void testBsonArraySize() {
    var element =
        new RawBsonDocument(
            new BsonDocument(
                UUID.randomUUID().toString(), new BsonString(UUID.randomUUID().toString())),
            BsonUtils.BSON_DOCUMENT_CODEC);
    var builder = BsonArrayBuilder.withLimit(Bytes.ofMebi(1));
    for (int i = 0; i < 1111; i++) {
      builder.append(element);
      Assert.assertEquals(
          BsonUtils.bsonValueSerializedBytes(builder.build()), builder.getDataSize());
    }
  }
}
