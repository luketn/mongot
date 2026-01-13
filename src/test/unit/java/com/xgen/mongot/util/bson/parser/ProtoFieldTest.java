package com.xgen.mongot.util.bson.parser;

import com.xgen.proto.Greeting;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.Assert;
import org.junit.Test;

public class ProtoFieldTest {
  @Test
  public void testParsesFromDocument() throws Exception {
    var test =
        new FieldParseTestUtil<>(
            Greeting.newBuilder().setMessage("Hello!").build(),
            Greeting.newBuilder().setMessage("World!").build(),
            (name, doc, value) ->
                doc.append(name, new BsonDocument("message", new BsonString(value.getMessage()))),
            name ->
                Field.builder(name).protoField(Greeting::parseBsonFrom).disallowUnknownFields());
    test.test();
  }

  @Test
  public void testDisallowUnknownFields() throws Exception {
    var field =
        Field.builder("greeting")
            .protoField(Greeting::parseBsonFrom)
            .disallowUnknownFields()
            .optional()
            .noDefault();
    var doc =
        new BsonDocument(
            "greeting",
            new BsonDocument()
                .append("message", new BsonString("hello"))
                .append("unknownField", BsonBoolean.valueOf(true)));
    try (var parser = BsonDocumentParser.fromRoot(doc).build()) {
      Assert.assertThrows(BsonParseException.class, () -> parser.getField(field));
    }
  }

  @Test
  public void testAllowUnknownFields() throws Exception {
    var field =
        Field.builder("greeting")
            .protoField(Greeting::parseBsonFrom)
            .allowUnknownFields()
            .optional()
            .noDefault();
    var doc =
        new BsonDocument(
            "greeting",
            new BsonDocument()
                .append("message", new BsonString("hello"))
                .append("unknownField", BsonBoolean.valueOf(true)));
    try (var parser = BsonDocumentParser.fromRoot(doc).build()) {
      Assert.assertEquals(
          Greeting.newBuilder().setMessage("hello").build(), parser.getField(field).unwrap().get());
    }
  }
}
