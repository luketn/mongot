package com.xgen.mongot.util.bson.parser;

/**
 * Contains classes that represent the result from fields after they have been parsed by a
 * DocumentParser.
 */
public class ParsedField {

  public static class Required<T> {

    private final String field;
    private final T value;

    Required(String field, T value) {
      this.field = field;
      this.value = value;
    }

    public String getField() {
      return this.field;
    }

    public T unwrap() {
      return this.value;
    }
  }

  public static class Optional<T> {

    private final String field;
    private final java.util.Optional<T> value;

    Optional(String field, java.util.Optional<T> value) {
      this.field = field;
      this.value = value;
    }

    public String getField() {
      return this.field;
    }

    public java.util.Optional<T> unwrap() {
      return this.value;
    }
  }

  public static class WithDefault<T> {

    private final String field;
    private final java.util.Optional<T> value;
    private final T defaultValue;

    WithDefault(String field, java.util.Optional<T> value, T defaultValue) {
      this.field = field;
      this.value = value;
      this.defaultValue = defaultValue;
    }

    public String getField() {
      return this.field;
    }

    public T unwrap() {
      return this.value.orElse(this.defaultValue);
    }

    public boolean isPresent() {
      return this.value.isPresent();
    }

    public boolean isEmpty() {
      return this.value.isEmpty();
    }
  }
}
