package com.xgen.mongot.util.bson.parser;

import java.util.List;
import java.util.function.Function;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonValue;
import org.junit.Assert;
import org.junit.Test;

public class IntegerFieldTest {

  @Test
  public void parsesPositiveIntegers() throws Exception {
    assertParsesInteger(13);
  }

  @Test
  public void parsesNegativeIntegers() throws Exception {
    assertParsesInteger(-13);
  }

  @Test
  public void parsesZero() throws Exception {
    assertParsesInteger(0);
  }

  @Test
  public void testNumericValidations() throws Exception {
    var numericTest =
        new NumericFieldTestUtil<>(name -> Field.builder(name).intField(), Integer::intValue);

    numericTest.assertValidations();
  }

  @Test
  public void throwsExceptionIfNotInteger() throws Exception {
    var field = Field.builder("foo").intField().required();
    var doc = new BsonDocument("foo", new BsonDouble(13.2));

    try (var parser = BsonDocumentParser.fromRoot(doc).build()) {
      Assert.assertThrows(BsonParseException.class, () -> parser.getField(field));
    }
  }

  @Test
  public void throwsExceptionIfOverflows() throws Exception {
    var field = Field.builder("foo").intField().required();
    var doc = new BsonDocument("foo", new BsonDouble(Math.pow(2, 54)));

    try (var parser = BsonDocumentParser.fromRoot(doc).build()) {
      Assert.assertThrows(BsonParseException.class, () -> parser.getField(field));
    }
  }

  @Test
  public void throwsExceptionIfUnderflows() throws Exception {
    var field = Field.builder("foo").intField().required();
    var doc = new BsonDocument("foo", new BsonDouble(-1 * Math.pow(2, 54)));

    try (var parser = BsonDocumentParser.fromRoot(doc).build()) {
      Assert.assertThrows(BsonParseException.class, () -> parser.getField(field));
    }
  }

  private void assertParsesInteger(int value) throws Exception {
    List<Function<Integer, BsonValue>> converters =
        List.of(BsonDouble::new, BsonInt32::new, BsonInt64::new);

    for (var converter : converters) {
      var test =
          new FieldParseTestUtil<>(
              value,
              value + 1,
              (name, doc, i) -> doc.append(name, converter.apply(i)),
              name -> Field.builder(name).intField());
      test.test();
    }
  }

  @Test
  public void testEncodes() throws Exception {
    var test =
        new FieldEncodeTestUtil<>(13, new BsonInt32(13), name -> Field.builder(name).intField());
    test.test();
  }

  @Test
  public void testEncodesAsDouble() throws Exception {
    var test =
        new FieldEncodeTestUtil<>(
            13, new BsonDouble(13), name -> Field.builder(name).intField().encodeAsDouble());
    test.test();
  }
}
