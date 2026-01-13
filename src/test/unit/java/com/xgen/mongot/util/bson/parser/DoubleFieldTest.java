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

public class DoubleFieldTest {

  private static final FieldParseTestUtil.AddValueToDoc<Double> ADD_VALUE_TO_DOC =
      (name, doc, d) -> doc.append(name, new BsonDouble(d));

  private static DoubleField.FieldBuilder builderSupplier(String name) {
    return Field.builder(name).doubleField();
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
    assertParsesDouble(13.13f);
  }

  @Test
  public void parsesNegativeNumbers() throws Exception {
    assertParsesDouble(-13.13f);
  }

  @Test
  public void parsesZero() throws Exception {
    assertParsesInteger(0);
  }

  @Test
  public void parsesInfinity() throws Exception {
    var test =
        new FieldParseTestUtil<>(
            Double.POSITIVE_INFINITY, 1d, ADD_VALUE_TO_DOC, DoubleFieldTest::builderSupplier);
    test.test();
  }

  @Test
  public void parsesNegativeInfinity() throws Exception {
    var test =
        new FieldParseTestUtil<>(
            Double.NEGATIVE_INFINITY, 1d, ADD_VALUE_TO_DOC, DoubleFieldTest::builderSupplier);
    test.test();
  }

  @Test
  public void parsesNaN() throws Exception {
    var test =
        new FieldParseTestUtil<>(
            Double.NaN, 1d, ADD_VALUE_TO_DOC, DoubleFieldTest::builderSupplier);
    test.test();
  }

  @Test
  public void testNumericValidations() throws Exception {
    var numericTest =
        new NumericFieldTestUtil<>(name -> Field.builder(name).doubleField(), i -> (double) i);

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

  private void assertParsesInteger(double value) throws Exception {
    List<Function<Double, BsonValue>> converters =
        List.of(
            BsonDouble::new, d -> new BsonInt32(d.intValue()), d -> new BsonInt64(d.longValue()));

    assertParsesNumber(value, converters);
  }

  private void assertParsesDouble(double value) throws Exception {
    List<Function<Double, BsonValue>> converters = List.of(BsonDouble::new);

    assertParsesNumber(value, converters);
  }

  private void assertParsesNumber(double value, List<Function<Double, BsonValue>> converters)
      throws Exception {
    for (var converter : converters) {
      var test =
          new FieldParseTestUtil<>(
              value,
              value + 1,
              (name, doc, i) -> doc.append(name, converter.apply(i)),
              DoubleFieldTest::builderSupplier);
      test.test();
    }
  }

  @Test
  public void testEncodes() throws Exception {
    var test =
        new FieldEncodeTestUtil<>(13.0, new BsonDouble(13.0), DoubleFieldTest::builderSupplier);
    test.test();
  }
}
