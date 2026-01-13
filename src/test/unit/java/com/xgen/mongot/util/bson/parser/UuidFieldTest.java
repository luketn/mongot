package com.xgen.mongot.util.bson.parser;

import java.util.UUID;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.Assert;
import org.junit.Test;

public class UuidFieldTest {

  @Test
  public void testParsesBinary() throws Exception {
    var test =
        new FieldParseTestUtil<>(
            UUID.randomUUID(),
            UUID.randomUUID(),
            (name, doc, value) -> doc.append(name, new BsonBinary(value)),
            name -> Field.builder(name).uuidField());
    test.test();
  }

  @Test
  public void testParsesString() throws Exception {
    var test =
        new FieldParseTestUtil<>(
            UUID.randomUUID(),
            UUID.randomUUID(),
            (name, doc, value) -> doc.append(name, new BsonString(value.toString())),
            name -> Field.builder(name).uuidField());
    test.test();
  }

  @Test
  public void throwsExceptionIfInvalidStringFormat() throws Exception {
    var field = Field.builder("foo").uuidField().encodeAsString().required();
    var doc = new BsonDocument("foo", new BsonString("not-a-uuid"));

    try (var parser = BsonDocumentParser.fromRoot(doc).build()) {
      Assert.assertThrows(BsonParseException.class, () -> parser.getField(field));
    }
  }

  @Test
  public void testEncodesAsBinary() throws Exception {
    var uuid = UUID.randomUUID();
    var test =
        new FieldEncodeTestUtil<>(
            uuid, new BsonBinary(uuid), name -> Field.builder(name).uuidField());
    test.test();
  }

  @Test
  public void testEncodesAsString() throws Exception {
    var uuid = UUID.fromString("eb6c40ca-f25e-47e8-b48c-02a05b64a5aa");
    var test =
        new FieldEncodeTestUtil<>(
            uuid,
            new BsonString("eb6c40ca-f25e-47e8-b48c-02a05b64a5aa"),
            name -> Field.builder(name).uuidField().encodeAsString());
    test.test();
  }
}
