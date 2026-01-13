package com.xgen.mongot.util.bson.parser;

import static com.google.common.truth.Truth.assertThat;

import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.Assert;
import org.junit.Test;

public class EnumFieldTest {
  private enum TestEnum {
    FIRST_VALUE,
    @Deprecated
    SECOND_VALUE,
  }

  @Test
  public void testParsesLowerCamel() throws Exception {
    var test =
        new FieldParseTestUtil<>(
            TestEnum.FIRST_VALUE,
            TestEnum.SECOND_VALUE,
            (name, doc, value) -> doc.append(name, new BsonString(lowerCamelName(value))),
            name -> Field.builder(name).enumField(TestEnum.class).asCamelCase());
    test.test();
  }

  @Test
  public void testParsesUpperCamel() throws Exception {
    var test =
        new FieldParseTestUtil<>(
            TestEnum.FIRST_VALUE,
            TestEnum.SECOND_VALUE,
            (name, doc, value) -> doc.append(name, new BsonString(upperCamelName(value))),
            name -> Field.builder(name).enumField(TestEnum.class).asUpperCamelCase());
    test.test();
  }

  @Test
  public void testParsesUpperUnderScore() throws Exception {
    var test =
        new FieldParseTestUtil<>(
            TestEnum.FIRST_VALUE,
            TestEnum.SECOND_VALUE,
            (name, doc, value) -> doc.append(name, new BsonString(upperUnderscoreName(value))),
            name -> Field.builder(name).enumField(TestEnum.class).asUpperUnderscore());
    test.test();
  }

  @Test
  public void testParsesCaseInsensitive() throws Exception {
    var test =
        new FieldParseTestUtil<>(
            TestEnum.FIRST_VALUE,
            TestEnum.SECOND_VALUE,
            (name, doc, value) -> doc.append(name, new BsonString(caseInsensitiveName(value))),
            name -> Field.builder(name).enumField(TestEnum.class).asCaseInsensitive());
    test.test();
  }

  private String lowerCamelName(TestEnum value) {
    return switch (value) {
      case FIRST_VALUE -> "firstValue";
      case SECOND_VALUE -> "secondValue";
    };
  }

  private String upperCamelName(TestEnum value) {
    return switch (value) {
      case FIRST_VALUE -> "FirstValue";
      case SECOND_VALUE -> "SecondValue";
    };
  }

  private String upperUnderscoreName(TestEnum value) {
    return switch (value) {
      case FIRST_VALUE -> "FIRST_VALUE";
      case SECOND_VALUE -> "SECOND_VALUE";
    };
  }

  private String caseInsensitiveName(TestEnum value) {
    return switch (value) {
      case FIRST_VALUE -> "FIRST_value";
      case SECOND_VALUE -> "second_VALUE";
    };
  }

  @Test
  public void testIncorrectEnumMemberStyleThrowsException() {
    var field = Field.builder("myEnum").enumField(TestEnum.class).asCamelCase().required();
    var parser =
        BsonDocumentParser.fromRoot(
                new BsonDocument().append("myEnum", new BsonString("FIRST_VALUE")))
            .build();
    var e = Assert.assertThrows(BsonParseException.class, () -> parser.getField(field));
    assertThat(e).hasMessageThat().contains("must be one of [firstValue]");
  }

  @Test
  public void testEncodesAsLowerCamelCase() throws Exception {
    var test =
        new FieldEncodeTestUtil<>(
            TestEnum.FIRST_VALUE,
            new BsonString("firstValue"),
            name -> Field.builder(name).enumField(TestEnum.class).asCamelCase());
    test.test();
  }

  @Test
  public void testEncodesAsUpperCamelCase() throws Exception {
    var test =
        new FieldEncodeTestUtil<>(
            TestEnum.FIRST_VALUE,
            new BsonString("FirstValue"),
            name -> Field.builder(name).enumField(TestEnum.class).asUpperCamelCase());
    test.test();
  }

  @Test
  public void testEncodesAsUpperUnderscore() throws Exception {
    var test =
        new FieldEncodeTestUtil<>(
            TestEnum.FIRST_VALUE,
            new BsonString("FIRST_VALUE"),
            name -> Field.builder(name).enumField(TestEnum.class).asUpperUnderscore());
    test.test();
  }

  @Test
  public void testEncodeAsCaseInsensitive() {
    var test =
        new FieldEncodeTestUtil<>(
            TestEnum.FIRST_VALUE,
            new BsonString("FIRST_VALUE"),
            name -> Field.builder(name).enumField(TestEnum.class).asCaseInsensitive());
    test.test();
  }

  @Test
  public void testDeprecatedTypeDoesNotErrorOut() {
    var test =
        new FieldEncodeTestUtil<>(
            TestEnum.SECOND_VALUE,
            new BsonString("SECOND_VALUE"),
            name -> Field.builder(name).enumField(TestEnum.class).asCaseInsensitive());
    test.test();
  }

  @Test
  public void testInvalidTypeErrorExcludesDeprecatedTypeFromErrorMessage() {
    var field = Field.builder("myEnum").enumField(TestEnum.class).asCamelCase().required();
    var parser =
        BsonDocumentParser.fromRoot(
                new BsonDocument().append("myEnum", new BsonString("FIRST_VALUE")))
            .build();
    var e = Assert.assertThrows(BsonParseException.class, () -> parser.getField(field));
    assertThat(e).hasMessageThat().contains("must be one of [firstValue]");
  }

  @Test
  public void withFallback_invalidValue_returnsFallbackEnum() throws BsonParseException {
    var field =
        Field.builder("myEnum")
            .enumField(TestEnum.class)
            .withFallback(TestEnum.FIRST_VALUE)
            .asCamelCase()
            .required();
    var parser =
        BsonDocumentParser.fromRoot(
                new BsonDocument().append("myEnum", new BsonString("invalidValue")))
            .build();
    TestEnum result = parser.getField(field).unwrap();
    assertThat(result).isEqualTo(TestEnum.FIRST_VALUE);
  }
}
