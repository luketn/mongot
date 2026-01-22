package com.xgen.mongot.util.bson.parser;

import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonNull;
import org.junit.Assert;
import org.junit.Test;

public class BsonDocumentParserTest {

  @Test
  public void canParseEmptyDocumentWithNoFields() throws Exception {
    var doc = new BsonDocument();
    var parser = BsonDocumentParser.fromRoot(doc).build();
    parser.close();
  }

  @Test
  public void throwsExceptionIfUnparsedFields() {
    var doc = new BsonDocument("foo", new BsonNull());
    var parser = BsonDocumentParser.fromRoot(doc).build();

    Assert.assertThrows(BsonParseException.class, parser::close);
  }

  @Test
  public void doesNotThrowExceptionWhenAllFieldsAreParsed() throws Exception {
    var doc = new BsonDocument("foo", new BsonNull());
    try (var parser = BsonDocumentParser.fromRoot(doc).build()) {
      parser.getField(Field.builder("foo").booleanField().optional().noDefault());
    }
  }

  @Test
  public void doesNotThrowExceptionIfUnparsedFieldsWithAllowUnknownFields() throws Exception {
    var doc = new BsonDocument("foo", new BsonNull());
    var parser = BsonDocumentParser.fromRoot(doc).allowUnknownFields(true).build();

    parser.close();
  }

  @Test
  public void throwsExceptionIfRequiredFieldIsMissing() throws Exception {
    try (var parser = BsonDocumentParser.fromRoot(new BsonDocument()).build()) {
      BsonParseException e =
          Assert.assertThrows(
              BsonParseException.class,
              () -> parser.getField(Field.builder("foo").stringField().required()));

      Assert.assertEquals("\"foo\" is required", e.getMessage());
    }
  }

  @Test
  public void doesNotThrowExceptionIfOptionalFieldIsMissing() throws Exception {
    try (var parser = BsonDocumentParser.fromRoot(new BsonDocument()).build()) {
      Assert.assertEquals(
          Optional.empty(),
          parser.getField(Field.builder("foo").stringField().optional().noDefault()).unwrap());
    }
  }
}
