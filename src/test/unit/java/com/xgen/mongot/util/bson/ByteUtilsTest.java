package com.xgen.mongot.util.bson;

import java.util.Arrays;
import org.apache.lucene.util.BytesRef;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.RawBsonDocument;
import org.junit.Assert;
import org.junit.Test;

public class ByteUtilsTest {
  @Test
  public void testDocumentIsConvertedToBytesWithNoOverhead() {
    BsonDocument document = new BsonDocument("a", new BsonBoolean(true));
    // 4 bytes for doc size, 2 bytes for string "a" with null termination,
    // 2 bytes for boolean with null termination, 1 byte for EOO
    // see https://bsonspec.org/spec.html for details
    Assert.assertEquals(4 + 2 + 2 + 1, ByteUtils.toByteArray(document).length);
  }

  @Test
  public void toBytesRef() {
    RawBsonDocument doc = RawBsonDocument.parse("{x: [1,2,3]}");
    byte[] raw = ByteUtils.toByteArray(doc);

    BytesRef bytesRef = ByteUtils.toBytesRef(doc);

    Assert.assertEquals(raw.length, bytesRef.length);
  }

  @Test
  public void fromBytesRef() {
    RawBsonDocument doc = RawBsonDocument.parse("{x: [1,2,3]}");
    byte[] original = ByteUtils.toByteArray(doc);
    byte[] copyToModify = original.clone();
    BytesRef wrapper = new BytesRef(copyToModify);

    RawBsonDocument recovered = ByteUtils.fromBytesRef(wrapper);
    Arrays.fill(copyToModify, (byte) -1); // Should not modify recovered
    byte[] recoveredBytes = ByteUtils.toByteArray(recovered);

    Assert.assertArrayEquals(original, recoveredBytes);
  }
}
