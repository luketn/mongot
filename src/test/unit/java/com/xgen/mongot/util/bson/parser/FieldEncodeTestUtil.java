package com.xgen.mongot.util.bson.parser;

import java.util.Optional;
import java.util.function.Function;
import org.bson.BsonNull;
import org.bson.BsonValue;
import org.junit.Assert;

class FieldEncodeTestUtil<T, B extends Field.TypedBuilder<T, B>> {

  private final T value;
  private final BsonValue expected;

  /** A function that returns a builder for the type being tested for a given field name. */
  private final Function<String, B> builderSupplier;

  FieldEncodeTestUtil(T value, BsonValue expected, Function<String, B> builderSupplier) {
    this.value = value;
    this.expected = expected;
    this.builderSupplier = builderSupplier;
  }

  public void test() {
    assertEncodesRequiredField();
    assertEncodesOptionalFieldWithValue();
    assertEncodesOptionalFieldWithoutValue();
    assertEncodesDefaultWithoutValue();
  }

  private void assertEncodesRequiredField() {
    var field = this.builderSupplier.apply("foo").required();
    Assert.assertEquals(this.expected, field.encode(this.value));
  }

  private void assertEncodesOptionalFieldWithValue() {
    var field = this.builderSupplier.apply("foo").optional().noDefault();
    Assert.assertEquals(this.expected, field.encode(Optional.of(this.value)));
  }

  private void assertEncodesOptionalFieldWithoutValue() {
    var field = this.builderSupplier.apply("foo").optional().noDefault();
    Assert.assertEquals(BsonNull.VALUE, field.encode(Optional.empty()));
  }

  private void assertEncodesDefaultWithoutValue() {
    var field = this.builderSupplier.apply("foo").optional().withDefault(this.value);
    Assert.assertEquals(this.expected, field.encode(this.value));
  }
}
