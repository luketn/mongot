package com.xgen.mongot.util.bson.parser;

import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.types.Decimal128;
import org.junit.Assert;
import org.junit.Test;

public class BsonNumberFieldTest {
  @Test
  public void testParsesInt32() throws Exception {
    var test =
        new FieldParseTestUtil<>(
            new BsonInt32(1),
            new BsonInt32(2),
            (name, doc, value) -> doc.append(name, value),
            name -> Field.builder(name).bsonNumberField());
    test.test();
  }

  @Test
  public void testParsesInt64() throws Exception {
    var test =
        new FieldParseTestUtil<>(
            new BsonInt64(1),
            new BsonInt64(2),
            (name, doc, value) -> doc.append(name, value),
            name -> Field.builder(name).bsonNumberField());
    test.test();
  }

  @Test
  public void testParsesDoubles() throws Exception {
    var test =
        new FieldParseTestUtil<>(
            new BsonDouble(1),
            new BsonDouble(2),
            (name, doc, value) -> doc.append(name, value),
            name -> Field.builder(name).bsonNumberField());
    test.test();
  }

  @Test
  public void testErrorDecimal128() throws Exception {
    var field = Field.builder("foo").bsonNumberField().required();
    var doc = new BsonDocument().append("foo", new BsonDecimal128(new Decimal128(1)));

    Assert.assertThrows(
        BsonParseException.class, () -> BsonDocumentParser.fromRoot(doc).build().getField(field));
  }

  @Test
  public void testEncodesInt32() throws Exception {
    var test =
        new FieldEncodeTestUtil<>(
            new BsonInt32(1), new BsonInt32(1), name -> Field.builder(name).bsonNumberField());
    test.test();
  }

  @Test
  public void testEncodesInt64() throws Exception {
    var test =
        new FieldEncodeTestUtil<>(
            new BsonInt64(1), new BsonInt64(1), name -> Field.builder(name).bsonNumberField());
    test.test();
  }

  @Test
  public void testEncodesDoubles() throws Exception {
    var test =
        new FieldEncodeTestUtil<>(
            new BsonDouble(1), new BsonDouble(1), name -> Field.builder(name).bsonNumberField());
    test.test();
  }
}
