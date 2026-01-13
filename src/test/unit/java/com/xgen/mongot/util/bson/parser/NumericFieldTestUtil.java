package com.xgen.mongot.util.bson.parser;

import java.util.function.Function;
import org.apache.commons.lang3.Range;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.junit.Assert;

class NumericFieldTestUtil<
    N extends Number & Comparable<N>, B extends NumericField.FieldBuilder<N, B>> {

  private final Function<String, B> fieldBuilderProvider;
  private final Range<N> zeroToTen;

  NumericFieldTestUtil(
      Function<String, B> fieldBuilderProvider, Function<Integer, N> intConverter) {
    this.fieldBuilderProvider = fieldBuilderProvider;
    this.zeroToTen = Range.of(intConverter.apply(0), intConverter.apply(10));
  }

  void assertValidations() throws Exception {
    assertMustBeNonNegativeOkayIfPositive();
    assertMustBeNonNegativeOkayIfZero();
    assertMustBeNonNegativeThrowsIfNegative();
    assertMustBePositiveOkayIfPositive();
    assertMustBePositiveThrowsIfZero();
    assertMustBePositiveThrowsIfNegative();
    assertMustBeInBoundsOkayIfInBounds();
    assertMustBeInBoundsThrowsIfOutOfBounds();
  }

  private void assertMustBeNonNegativeOkayIfPositive() throws Exception {
    var field = this.fieldBuilderProvider.apply("foo").mustBeNonNegative().required();
    var doc = new BsonDocument("foo", new BsonDouble(13));

    try (var parser = BsonDocumentParser.fromRoot(doc).build()) {
      parser.getField(field);
    }
  }

  private void assertMustBeNonNegativeOkayIfZero() throws Exception {
    var field = this.fieldBuilderProvider.apply("foo").mustBeNonNegative().required();
    var doc = new BsonDocument("foo", new BsonDouble(0));

    try (var parser = BsonDocumentParser.fromRoot(doc).build()) {
      parser.getField(field);
    }
  }

  private void assertMustBeNonNegativeThrowsIfNegative() throws Exception {
    var field = this.fieldBuilderProvider.apply("foo").mustBeNonNegative().required();
    var doc = new BsonDocument("foo", new BsonDouble(-13));

    try (var parser = BsonDocumentParser.fromRoot(doc).build()) {
      Assert.assertThrows(BsonParseException.class, () -> parser.getField(field));
    }
  }

  private void assertMustBePositiveOkayIfPositive() throws Exception {
    var field = this.fieldBuilderProvider.apply("foo").mustBePositive().required();
    var doc = new BsonDocument("foo", new BsonDouble(13));

    try (var parser = BsonDocumentParser.fromRoot(doc).build()) {
      parser.getField(field);
    }
  }

  private void assertMustBePositiveThrowsIfZero() throws Exception {
    var field = this.fieldBuilderProvider.apply("foo").mustBePositive().required();
    var doc = new BsonDocument("foo", new BsonDouble(0));

    try (var parser = BsonDocumentParser.fromRoot(doc).build()) {
      Assert.assertThrows(BsonParseException.class, () -> parser.getField(field));
    }
  }

  private void assertMustBePositiveThrowsIfNegative() throws Exception {
    var field = this.fieldBuilderProvider.apply("foo").mustBePositive().required();
    var doc = new BsonDocument("foo", new BsonDouble(-13));

    try (var parser = BsonDocumentParser.fromRoot(doc).build()) {
      Assert.assertThrows(BsonParseException.class, () -> parser.getField(field));
    }
  }

  private void assertMustBeInBoundsOkayIfInBounds() throws Exception {
    var field =
        this.fieldBuilderProvider.apply("foo").mustBeWithinBounds(this.zeroToTen).required();
    var doc = new BsonDocument("foo", new BsonDouble(5));

    try (var parser = BsonDocumentParser.fromRoot(doc).build()) {
      parser.getField(field);
    }
  }

  private void assertMustBeInBoundsThrowsIfOutOfBounds() throws Exception {
    var field =
        this.fieldBuilderProvider.apply("foo").mustBeWithinBounds(this.zeroToTen).required();
    var doc = new BsonDocument("foo", new BsonDouble(11));

    try (var parser = BsonDocumentParser.fromRoot(doc).build()) {
      Assert.assertThrows(BsonParseException.class, () -> parser.getField(field));
    }
  }
}
