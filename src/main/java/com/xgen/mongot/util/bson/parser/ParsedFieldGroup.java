package com.xgen.mongot.util.bson.parser;

import com.xgen.mongot.util.Optionals;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ParsedFieldGroup {

  private final BsonParseContext context;

  ParsedFieldGroup(BsonParseContext context) {
    this.context = context;
  }

  /**
   * Ensures that only one of the supplied ParsedField.Optional values actually has a value present,
   * and returns the unwrapped value of that field.
   *
   * <p>If multiple or no fields were present, throws a BsonParseException.
   */
  @SafeVarargs
  public final <V> V exactlyOneOf(ParsedField.Optional<? extends V>... optionals)
      throws BsonParseException {
    @SuppressWarnings("varargs")
    List<V> present = getPresent(optionals);

    if (present.size() == 0) {
      this.context.handleSemanticError(
          String.format("one of [%s] must be present", fieldNames(optionals)));
    }

    if (present.size() != 1) {
      this.context.handleSemanticError(
          String.format("only one of [%s] may be present", fieldNames(optionals)));
    }

    return present.get(0);
  }

  /**
   * Ensures that no more than one of the supplied ParsedField.Optional values actually has a value
   * present, and returns the unwrapped value of that field if there was one.
   *
   * <p>If multiple fields were present, throws a BsonParseException.
   */
  @SafeVarargs
  public final <V> Optional<V> atMostOneOf(ParsedField.Optional<? extends V>... optionals)
      throws BsonParseException {
    @SuppressWarnings("varargs")
    List<V> present = getPresent(optionals);

    if (present.size() == 0) {
      return Optional.empty();
    }

    if (present.size() != 1) {
      this.context.handleSemanticError(
          String.format("at most one of [%s] may be present", fieldNames(optionals)));
    }

    return Optional.of(present.get(0));
  }

  /**
   * Ensures that at least one of the supplied ParsedField.Optional values actually has a value
   * present.
   *
   * <p>If no fields were present, throws a BsonParseException.
   */
  @SafeVarargs
  public final <V> List<V> atLeastOneOf(ParsedField.Optional<? extends V>... optionals)
      throws BsonParseException {
    @SuppressWarnings("varargs")
    List<V> present = getPresent(optionals);

    if (present.size() == 0) {
      this.context.handleSemanticError(
          String.format("at least one of [%s] must be present", fieldNames(optionals)));
    }
    return present;
  }

  @SafeVarargs
  private static <V> List<V> getPresent(ParsedField.Optional<? extends V>... optionals) {
    return Optionals.present(Stream.of(optionals).map(ParsedField.Optional::unwrap))
        .collect(Collectors.toList());
  }

  private static String fieldNames(ParsedField.Optional<?>... optionals) {
    return Stream.of(optionals)
        .map(ParsedField.Optional::getField)
        .collect(Collectors.joining(", "));
  }
}
