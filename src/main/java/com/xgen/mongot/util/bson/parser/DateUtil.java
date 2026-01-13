package com.xgen.mongot.util.bson.parser;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Optional;

public final class DateUtil {

  public static Optional<Instant> parseInstantFromString(
      DocumentParser parser, DateTimeFormatter formatter, Field.Optional<String> field)
      throws BsonParseException {
    try {
      Optional<String> definitionCreatedAtAsString = parser.getField(field).unwrap();
      return definitionCreatedAtAsString.map(string -> Instant.from(formatter.parse(string)));
    } catch (DateTimeParseException e) {
      return parser
          .getContext()
          .child(field.getName())
          .handleSemanticError(String.format("could not be parsed: %s", e.getMessage()));
    }
  }
}
