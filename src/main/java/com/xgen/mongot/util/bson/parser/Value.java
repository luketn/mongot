package com.xgen.mongot.util.bson.parser;

/** Value is similar to Field except that it does not have a field name associated with it. */
public class Value {

  public static class Required<T> {

    private final ValueEncoder<T> encoder;
    private final ValueParser<T> parser;

    Required(ValueEncoder<T> encoder, ValueParser<T> parser) {
      this.encoder = encoder;
      this.parser = parser;
    }

    public ValueEncoder<T> getEncoder() {
      return this.encoder;
    }

    public ValueParser<T> getParser() {
      return this.parser;
    }
  }

  public static class Optional<T> {

    private final ValueEncoder<java.util.Optional<T>> encoder;
    private final ValueParser<java.util.Optional<T>> parser;

    Optional(
        ValueEncoder<java.util.Optional<T>> encoder, ValueParser<java.util.Optional<T>> parser) {
      this.encoder = encoder;
      this.parser = parser;
    }

    public ValueEncoder<java.util.Optional<T>> getEncoder() {
      return this.encoder;
    }

    public ValueParser<java.util.Optional<T>> getParser() {
      return this.parser;
    }
  }

  public static class WithDefault<T> {

    private final ValueEncoder<java.util.Optional<T>> encoder;
    private final ValueParser<java.util.Optional<T>> parser;
    private final T defaultValue;

    WithDefault(
        ValueEncoder<java.util.Optional<T>> encoder,
        ValueParser<java.util.Optional<T>> parser,
        T defaultValue) {
      this.encoder = encoder;
      this.parser = parser;
      this.defaultValue = defaultValue;
    }

    public ValueEncoder<java.util.Optional<T>> getEncoder() {
      return this.encoder;
    }

    public ValueParser<java.util.Optional<T>> getParser() {
      return this.parser;
    }

    public T getDefaultValue() {
      return this.defaultValue;
    }
  }

  public static TypeBuilder builder() {
    return new TypeBuilder();
  }

  /** A builder stage that requires the type being parsed to be specified. */
  public static class TypeBuilder {

    public BooleanField.ValueBuilder booleanValue() {
      return new BooleanField.ValueBuilder();
    }

    public <T extends Encodable> ClassField.DocumentValueBuilder<T> classValue(
        ClassField.FromDocumentParser<T> parser) {
      return new ClassField.DocumentValueBuilder<>(Encodable::toBson, parser);
    }

    public <T extends Encodable> ClassField.ValueBuilder<T> classValue(
        ClassField.FromValueParser<T> parser) {
      return new ClassField.ValueBuilder<>(Encodable::toBson, parser);
    }

    public DocumentField.ValueBuilder documentValue() {
      return new DocumentField.ValueBuilder();
    }

    public BinaryField.ValueBuilder binaryField() {
      return new BinaryField.ValueBuilder();
    }

    public BsonDateTimeField.ValueBuilder bsonDateTimeField() {
      return new BsonDateTimeField.ValueBuilder();
    }

    public BsonNumberField.ValueBuilder bsonNumberField() {
      return new BsonNumberField.ValueBuilder();
    }

    public BsonTimestampField.ValueBuilder bsonTimestampField() {
      return new BsonTimestampField.ValueBuilder();
    }

    public DoubleField.ValueBuilder doubleValue() {
      return new DoubleField.ValueBuilder();
    }

    public LongField.ValueBuilder longValue() {
      return new LongField.ValueBuilder();
    }

    public FloatField.ValueBuilder floatValue() {
      return new FloatField.ValueBuilder();
    }

    public IntegerField.ValueBuilder intValue() {
      return new IntegerField.ValueBuilder();
    }

    public ObjectIdField.ValueBuilder objectIdValue() {
      return new ObjectIdField.ValueBuilder();
    }

    public StringField.ValueBuilder stringValue() {
      return new StringField.ValueBuilder();
    }

    public <T extends Enum<T>> EnumField.ValueCaseSelector<T> enumValue(Class<T> enumClass) {
      return new EnumField.ValueCaseSelector<>(enumClass);
    }

    public UuidField.ValueBuilder uuidValue() {
      return new UuidField.ValueBuilder();
    }

    public UnparsedValueField.ValueBuilder unparsedValueField() {
      return new UnparsedValueField.ValueBuilder();
    }

    public <T> ListField.ValueBuilder<T> listOf(Value.Required<T> element) {
      return new ListField.ValueBuilder<>(
          element.getEncoder(), element.getParser(), ListField.SingleValuePresence.MUST_BE_LIST);
    }

    public <T> ListField.ValueBuilder<java.util.Optional<T>> listOf(Value.Optional<T> element) {
      return new ListField.ValueBuilder<>(
          element.getEncoder(), element.getParser(), ListField.SingleValuePresence.MUST_BE_LIST);
    }

    /**
     * Creates a list of the supplied element, allowing the parsed value to be a single value of the
     * element type.
     */
    public <T> ListField.ValueBuilder<T> singleValueOrListOf(Value.Required<T> element) {
      return new ListField.ValueBuilder<>(
          element.getEncoder(),
          element.getParser(),
          ListField.SingleValuePresence.SINGLE_VALUE_ALLOWED);
    }

    /**
     * Creates a list of the supplied element, allowing the parsed value to be a single value of the
     * element type.
     */
    public <T> ListField.ValueBuilder<java.util.Optional<T>> singleValueOrListOf(
        Value.Optional<T> element) {
      return new ListField.ValueBuilder<>(
          element.getEncoder(),
          element.getParser(),
          ListField.SingleValuePresence.SINGLE_VALUE_ALLOWED);
    }

    /**
     * Creates a list of the supplied element, allowing the parsed value to be a single value of the
     * element type.
     */
    public <T> ListField.ValueBuilder<java.util.Optional<T>> singleValueOrListOf(
        Value.WithDefault<T> element) {
      return new ListField.ValueBuilder<>(
          element.getEncoder(),
          element.getParser(),
          ListField.SingleValuePresence.SINGLE_VALUE_ALLOWED);
    }

    public <T> MapField.ValueBuilder<T> mapOf(Value.Required<T> value) {
      return MapField.ValueBuilder.create(value.getEncoder(), value.getParser());
    }

    public <T> MapField.ValueBuilder<java.util.Optional<T>> mapOf(Value.Optional<T> value) {
      return MapField.ValueBuilder.create(value.getEncoder(), value.getParser());
    }

    public <T> MapField.ValueBuilder<java.util.Optional<T>> mapOf(Value.WithDefault<T> value) {
      return MapField.ValueBuilder.create(value.getEncoder(), value.getParser());
    }
  }

  /**
   * Each type with built in support for parsing should supply a builder which can be returned by
   * the TypeBuilder that supports type-specific validations, as well as the methods below.
   *
   * <p>Each type should extend TypeBuilder, where T is the type of field that will be built from
   * it.
   *
   * <p>B is the actual class that extends TypeBuilder. By having B specified, TypeBuilder can
   * return that specific class B rather than just a TypeBuilder over T, allowing for the
   * type-specific validation methods to be visible.
   */
  abstract static class TypedBuilder<T, B extends TypedBuilder<T, B>> {

    final ValueEncoder<T> encoder;
    final ValueParser<T> parser;

    TypedBuilder(ValueEncoder<T> encoder, ValueParser<T> parser) {
      this.encoder = encoder;
      this.parser = parser;
    }

    interface BuilderFactory<T, B> {
      B getBuilder(ValueEncoder<T> encoder, ValueParser<T> parser);
    }

    abstract BuilderFactory<T, B> getBuilderFactory();

    public B validate(FieldValidator<T> validator) {
      ValueParser<T> parser = new ValidatingValueParser<>(this.parser, validator);
      return getBuilderFactory().getBuilder(this.encoder, parser);
    }

    public Value.Required<T> required() {
      return new Value.Required<>(this.encoder, this.parser);
    }

    public OptionalField.ValueBuilder<T> optional() {
      return new OptionalField.ValueBuilder<>(this.encoder, this.parser);
    }

    public ListField.ValueBuilder<T> asList() {
      return new ListField.ValueBuilder<>(
          this.encoder, this.parser, ListField.SingleValuePresence.MUST_BE_LIST);
    }

    public ListField.ValueBuilder<T> asSingleValueOrList() {
      return new ListField.ValueBuilder<>(
          this.encoder, this.parser, ListField.SingleValuePresence.SINGLE_VALUE_ALLOWED);
    }

    public MapField.ValueBuilder<T> asMap() {
      return MapField.ValueBuilder.create(this.encoder, this.parser);
    }
  }
}
