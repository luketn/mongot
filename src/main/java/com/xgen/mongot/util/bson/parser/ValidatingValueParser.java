package com.xgen.mongot.util.bson.parser;

import java.util.Optional;
import org.bson.BsonValue;

/**
 * ValidatingValueParser wraps an existing ValueParser and checks if the value produced by the
 * wrapped ValueParser passes the FieldValidator.
 */
class ValidatingValueParser<T> implements ValueParser<T> {

  private final ValueParser<T> wrapped;
  private final FieldValidator<T> validator;

  ValidatingValueParser(ValueParser<T> wrapped, FieldValidator<T> validator) {
    this.wrapped = wrapped;
    this.validator = validator;
  }

  @Override
  public T parse(BsonParseContext context, BsonValue value) throws BsonParseException {
    T parsed = this.wrapped.parse(context, value);
    Optional<String> error = this.validator.validate(parsed);
    if (error.isPresent()) {
      context.handleSemanticError(error.get());
    }

    return parsed;
  }
}
