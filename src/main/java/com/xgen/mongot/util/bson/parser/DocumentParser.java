package com.xgen.mongot.util.bson.parser;

public interface DocumentParser extends AutoCloseable {

  /**
   * Attempts to parse a required field, throwing a BsonParseException if it does not exist or is
   * invalid.
   */
  <T> ParsedField.Required<T> getField(Field.Required<T> field) throws BsonParseException;

  /** Attempts to parse an optional field, throwing a BsonParseException if it is invalid. */
  <T> ParsedField.Optional<T> getField(Field.Optional<T> field) throws BsonParseException;

  /**
   * Attempts to parse an optional field with a default, throwing a BsonParseException if it is
   * invalid.
   */
  <T> ParsedField.WithDefault<T> getField(Field.WithDefault<T> field) throws BsonParseException;

  /** checks if the field is present without making an attempt to parse it */
  boolean hasField(Field.Optional<?> field) throws BsonParseException;

  /** Returns a ParsedFieldGroup that can be used to check field group semantics. */
  ParsedFieldGroup getGroup();

  /** Returns the BsonParseContext that the DocumentParser is using. */
  BsonParseContext getContext();

  /**
   * Ensures that all fields in the document have been read via getField() (i.e. that there are no
   * unexpected fields).
   */
  @Override
  void close() throws BsonParseException;
}
