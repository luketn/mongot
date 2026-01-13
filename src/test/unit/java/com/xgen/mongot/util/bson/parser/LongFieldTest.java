package com.xgen.mongot.util.bson.parser;

import java.util.List;
import java.util.function.Function;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt64;
import org.bson.BsonValue;
import org.junit.Assert;
import org.junit.Test;

public class LongFieldTest {

  @Test
  public void parsesPositiveLong() throws Exception {
    assertParsesLong(13L);
  }

  @Test
  public void parsesNegativeLong() throws Exception {
    assertParsesLong(-13L);
  }

  @Test
  public void parsesLargeDoubleDoesNotThrowException() throws Exception {
    var field = Field.builder("foo").longField().required();
    var doc = new BsonDocument("foo", new BsonDouble(Math.pow(2, 53) + 0.1));

    try (var parser = BsonDocumentParser.fromRoot(doc).build()) {
      long l = parser.getField(field).unwrap();
      // we only compare to 2^53 as the 0.1 will get truncated and not trigger the % check in
      // LongField.java.
      Assert.assertEquals(l, (long) Math.pow(2, 53));
    }
  }

  @Test
  public void parsesLargeDouble() throws Exception {
    var field = Field.builder("foo").longField().required();
    var doc = new BsonDocument("foo", new BsonDouble(Math.pow(2, 60)));

    try (var parser = BsonDocumentParser.fromRoot(doc).build()) {
      long l = parser.getField(field).unwrap();

      Assert.assertEquals(l, (long) Math.pow(2, 60));
    }
  }

  @Test
  public void parsesDouble() throws Exception {
    var field = Field.builder("foo").longField().required();
    var doc = new BsonDocument("foo", new BsonDouble(12.0));

    try (var parser = BsonDocumentParser.fromRoot(doc).build()) {
      long l = parser.getField(field).unwrap();

      Assert.assertEquals(12, l);
    }
  }

  @Test
  public void throwsExceptionForSmallDoubles() throws Exception {
    var field = Field.builder("foo").longField().required();
    var doc = new BsonDocument("foo", new BsonDouble(Math.pow(2, -1074)));

    try (var parser = BsonDocumentParser.fromRoot(doc).build()) {
      Assert.assertThrows(BsonParseException.class, () -> parser.getField(field));
    }
  }

  @Test
  public void parsesZero() throws Exception {
    assertParsesLong(0);
  }

  @Test
  public void parsesNegativeDoubleZero() throws Exception {
    var field = Field.builder("foo").longField().required();
    var doc = new BsonDocument("foo", new BsonDouble(-0.0));

    try (var parser = BsonDocumentParser.fromRoot(doc).build()) {
      long l = parser.getField(field).unwrap();

      Assert.assertEquals(0, l);
    }
  }

  @Test
  public void doesNotParsePositiveInfinity() throws Exception {
    var field = Field.builder("foo").longField().required();
    var doc = new BsonDocument("foo", new BsonDouble(Double.POSITIVE_INFINITY));

    try (var parser = BsonDocumentParser.fromRoot(doc).build()) {
      Assert.assertThrows(BsonParseException.class, () -> parser.getField(field));
    }
  }

  @Test
  public void doesNotParseNegativeInfinity() throws Exception {
    var field = Field.builder("foo").longField().required();
    var doc = new BsonDocument("foo", new BsonDouble(Double.NEGATIVE_INFINITY));

    try (var parser = BsonDocumentParser.fromRoot(doc).build()) {
      Assert.assertThrows(BsonParseException.class, () -> parser.getField(field));
    }
  }

  @Test
  public void doesNotParseNaN() throws Exception {
    var field = Field.builder("foo").longField().required();
    var doc = new BsonDocument("foo", new BsonDouble(Double.NaN));

    try (var parser = BsonDocumentParser.fromRoot(doc).build()) {
      Assert.assertThrows(BsonParseException.class, () -> parser.getField(field));
    }
  }

  @Test
  public void parsesIntegers() throws Exception {
    assertParsesLong(3);
  }

  @Test
  public void testNumericValidations() throws Exception {
    var numericTest =
        new NumericFieldTestUtil<>(name -> Field.builder(name).longField(), i -> (long) i);

    numericTest.assertValidations();
  }

  @Test
  public void throwsExceptionIfNotLong() throws Exception {
    var field = Field.builder("foo").longField().required();
    var doc = new BsonDocument("foo", new BsonDouble(13.3));

    try (var parser = BsonDocumentParser.fromRoot(doc).build()) {
      Assert.assertThrows(BsonParseException.class, () -> parser.getField(field));
    }
  }

  @Test
  public void throwsExceptionIfOverflows() throws Exception {
    var field = Field.builder("foo").longField().required();
    var doc = new BsonDocument("foo", new BsonDouble(Math.pow(2, 64)));

    try (var parser = BsonDocumentParser.fromRoot(doc).build()) {
      Assert.assertThrows(BsonParseException.class, () -> parser.getField(field));
    }
  }

  @Test
  public void throwsExceptionIfUnderflows() throws Exception {
    var field = Field.builder("foo").longField().required();
    var doc = new BsonDocument("foo", new BsonDouble(-1 * Math.pow(2, 64)));

    try (var parser = BsonDocumentParser.fromRoot(doc).build()) {
      Assert.assertThrows(BsonParseException.class, () -> parser.getField(field));
    }
  }

  private void assertParsesLong(long value) throws Exception {
    List<Function<Long, BsonValue>> converters = List.of(BsonDouble::new, BsonInt64::new);

    for (var converter : converters) {
      var test =
          new FieldParseTestUtil<>(
              value,
              value + 1,
              (name, doc, i) -> doc.append(name, converter.apply(i)),
              name -> Field.builder(name).longField());
      test.test();
    }
  }

  @Test
  public void testEncodes() throws Exception {
    var test =
        new FieldEncodeTestUtil<>(13L, new BsonInt64(13L), name -> Field.builder(name).longField());
    test.test();
  }
}
