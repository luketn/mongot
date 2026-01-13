package com.xgen.mongot.util.bson.parser;

import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.Assert;
import org.junit.Test;

public class StringFieldTest {

  @Test
  public void testParses() throws Exception {
    var test =
        new FieldParseTestUtil<>(
            "foo",
            "bar",
            (name, doc, value) -> doc.append(name, new BsonString(value)),
            name -> Field.builder(name).stringField());
    test.test();
  }

  @Test
  public void testNotEmpty() throws Exception {
    var field = Field.builder("foo").stringField().mustNotBeEmpty().required();
    var doc = new BsonDocument("foo", new BsonString(""));

    try (var parser = BsonDocumentParser.fromRoot(doc).build()) {
      Assert.assertThrows(BsonParseException.class, () -> parser.getField(field));
    }
  }

  @Test
  public void testNotBlank() throws Exception {
    var field = Field.builder("foo").stringField().mustNotBeBlank().required();
    var emptyDoc = new BsonDocument("foo", new BsonString(""));
    var blankDoc = new BsonDocument("foo", new BsonString("  "));

    try (var parser = BsonDocumentParser.fromRoot(emptyDoc).build()) {
      Assert.assertThrows(BsonParseException.class, () -> parser.getField(field));
    }

    try (var parser = BsonDocumentParser.fromRoot(blankDoc).build()) {
      Assert.assertThrows(BsonParseException.class, () -> parser.getField(field));
    }
  }

  @Test
  public void testNotBeginsWith() throws Exception {
    var field = Field.builder("foo").stringField().mustNotBeginWith("bar").required();
    var doc = new BsonDocument("foo", new BsonString("bar"));

    try (var parser = BsonDocumentParser.fromRoot(doc).build()) {
      Assert.assertThrows(BsonParseException.class, () -> parser.getField(field));
    }
  }

  @Test
  public void testEncodes() throws Exception {
    var test =
        new FieldEncodeTestUtil<>(
            "foo", new BsonString("foo"), name -> Field.builder(name).stringField());
    test.test();
  }
}
