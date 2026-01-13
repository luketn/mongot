package com.xgen.mongot.util.bson.parser;

import java.util.function.Function;
import org.bson.BsonDocument;
import org.junit.Assert;

class FieldParseTestUtil<T, B extends Field.TypedBuilder<T, B>> {

  interface AddValueToDoc<T> {
    BsonDocument addValue(String name, BsonDocument doc, T value);
  }

  /** A value of the type that is being tested. */
  private final T first;

  /** A separate value of the type that is being tested, must not be equal to first. */
  private final T second;

  /**
   * A function that adds a value of the type that is being tested to a document for a given field
   * name.
   */
  private final AddValueToDoc<T> addValueToDoc;

  /** A function that returns a builder for the type being tested for a given field name. */
  private final Function<String, B> builderSupplier;

  FieldParseTestUtil(
      T first, T second, AddValueToDoc<T> addValueToDoc, Function<String, B> builderSupplier) {
    this.first = first;
    this.second = second;
    this.addValueToDoc = addValueToDoc;
    this.builderSupplier = builderSupplier;
  }

  public void test() throws Exception {
    assertParsesRequiredField();
    assertParsesOptionalFieldWithValue();
    assertParsesOptionalFieldWithoutValue();
    assertParsesValueWithDefaultWithValue();
    assertParsesValueWithDefaultWithoutValue();
  }

  private void assertParsesRequiredField() throws Exception {
    var field = this.builderSupplier.apply("foo").required();
    var doc = this.addValueToDoc.addValue("foo", new BsonDocument(), this.first);

    try (var parser = BsonDocumentParser.fromRoot(doc).build()) {
      var value = parser.getField(field);
      Assert.assertEquals("foo", value.getField());
      Assert.assertEquals(this.first, value.unwrap());
    }
  }

  private void assertParsesOptionalFieldWithValue() throws Exception {
    var field = this.builderSupplier.apply("foo").optional().noDefault();
    var doc = this.addValueToDoc.addValue("foo", new BsonDocument(), this.first);

    try (var parser = BsonDocumentParser.fromRoot(doc).build()) {
      var value = parser.getField(field);
      Assert.assertEquals("foo", value.getField());
      Assert.assertTrue(value.unwrap().isPresent());
      Assert.assertEquals(this.first, value.unwrap().get());
    }
  }

  private void assertParsesOptionalFieldWithoutValue() throws Exception {
    var field = this.builderSupplier.apply("foo").optional().noDefault();
    var doc = new BsonDocument();

    try (var parser = BsonDocumentParser.fromRoot(doc).build()) {
      var value = parser.getField(field);
      Assert.assertEquals("foo", value.getField());
      Assert.assertTrue(value.unwrap().isEmpty());
    }
  }

  private void assertParsesValueWithDefaultWithValue() throws Exception {
    var field = this.builderSupplier.apply("foo").optional().withDefault(this.second);
    var doc = this.addValueToDoc.addValue("foo", new BsonDocument(), this.first);

    try (var parser = BsonDocumentParser.fromRoot(doc).build()) {
      var value = parser.getField(field);
      Assert.assertEquals("foo", value.getField());
      Assert.assertEquals(this.first, value.unwrap());
    }
  }

  private void assertParsesValueWithDefaultWithoutValue() throws Exception {
    var field = this.builderSupplier.apply("foo").optional().withDefault(this.second);
    var doc = new BsonDocument();

    try (var parser = BsonDocumentParser.fromRoot(doc).build()) {
      var value = parser.getField(field);
      Assert.assertEquals("foo", value.getField());
      Assert.assertEquals(this.second, value.unwrap());
    }
  }
}
