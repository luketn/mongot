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

public class FloatFieldTest {

  private static final FieldParseTestUtil.AddValueToDoc<Float> ADD_VALUE_TO_DOC =
      (name, doc, f) -> doc.append(name, new BsonDouble(f));

  private static FloatField.FieldBuilder builderSupplier(String name) {
    return Field.builder(name).floatField();
  }

  @Test
  public void parsesPositiveIntegers() throws Exception {
    assertParsesInteger(13);
  }

  @Test
  public void parsesNegativeIntegers() throws Exception {
    assertParsesInteger(-13);
  }

  @Test
  public void parsesPositiveNumbers() throws Exception {
    assertParsesFloat(13.13f);
  }

  @Test
  public void parsesNegativeNumbers() throws Exception {
    assertParsesFloat(-13.13f);
  }

  @Test
  public void parsesZero() throws Exception {
    assertParsesInteger(0);
  }

  @Test
  public void parsesInfinity() throws Exception {
    var test =
        new FieldParseTestUtil<>(
            Float.POSITIVE_INFINITY, 1f, ADD_VALUE_TO_DOC, FloatFieldTest::builderSupplier);
    test.test();
  }

  @Test
  public void parsesNegativeInfinity() throws Exception {
    var test =
        new FieldParseTestUtil<>(
            Float.NEGATIVE_INFINITY, 1f, ADD_VALUE_TO_DOC, FloatFieldTest::builderSupplier);
    test.test();
  }

  @Test
  public void parsesNaN() throws Exception {
    var test =
        new FieldParseTestUtil<>(Float.NaN, 1f, ADD_VALUE_TO_DOC, FloatFieldTest::builderSupplier);
    test.test();
  }

  @Test
  public void testNumericValidations() throws Exception {
    var numericTest =
        new NumericFieldTestUtil<>(name -> Field.builder(name).floatField(), i -> (float) i);

    numericTest.assertValidations();
  }

  @Test
  public void testMustBeFiniteThrowsIfNotFinite() throws Exception {
    var field = Field.builder("foo").doubleField().mustBeFinite().required();

    var finite = new BsonDocument("foo", new BsonDouble(13d));
    try (var parser = BsonDocumentParser.fromRoot(finite).build()) {
      // Shouldn't throw.
      parser.getField(field).unwrap();
    }

    var inf = new BsonDocument("foo", new BsonDouble(Double.POSITIVE_INFINITY));
    try (var parser = BsonDocumentParser.fromRoot(inf).build()) {
      Assert.assertThrows(BsonParseException.class, () -> parser.getField(field));
    }

    var negInf = new BsonDocument("foo", new BsonDouble(Double.NEGATIVE_INFINITY));
    try (var parser = BsonDocumentParser.fromRoot(negInf).build()) {
      Assert.assertThrows(BsonParseException.class, () -> parser.getField(field));
    }

    var nan = new BsonDocument("foo", new BsonDouble(Double.NaN));
    try (var parser = BsonDocumentParser.fromRoot(nan).build()) {
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

  private void assertParsesInteger(float value) throws Exception {
    List<Function<Float, BsonValue>> converters =
        List.of(
            BsonDouble::new, f -> new BsonInt32(f.intValue()), f -> new BsonInt64(f.longValue()));

    assertParsesNumber(value, converters);
  }

  private void assertParsesFloat(float value) throws Exception {
    List<Function<Float, BsonValue>> converters = List.of(BsonDouble::new);

    assertParsesNumber(value, converters);
  }

  private void assertParsesNumber(float value, List<Function<Float, BsonValue>> converters)
      throws Exception {
    for (var converter : converters) {
      var test =
          new FieldParseTestUtil<>(
              value,
              value + 1,
              (name, doc, i) -> doc.append(name, converter.apply(i)),
              FloatFieldTest::builderSupplier);
      test.test();
    }
  }

  @Test
  public void testEncodes() throws Exception {
    var test =
        new FieldEncodeTestUtil<>(13.0f, new BsonDouble(13.0), FloatFieldTest::builderSupplier);
    test.test();
  }
}
