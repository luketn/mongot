package com.xgen.mongot.util.bson.parser;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.bson.BsonDocument;
import org.bson.BsonElement;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.junit.Assert;
import org.junit.Test;

public class MapFieldTest {

  @Test
  public void parsesSingleValueList() throws Exception {
    var test =
        new FieldParseTestUtil<>(
            Map.ofEntries(Map.entry("foo", 1)),
            Map.ofEntries(Map.entry("bar", 2)),
            (name, doc, value) ->
                doc.append(
                    name,
                    new BsonDocument(
                        value.entrySet().stream()
                            .map(e -> new BsonElement(e.getKey(), new BsonInt32(e.getValue())))
                            .collect(Collectors.toList()))),
            name -> Field.builder(name).mapOf(Value.builder().intValue().required()));
    test.test();
  }

  @Test
  public void parsesMultiValueList() throws Exception {
    var test =
        new FieldParseTestUtil<>(
            Map.ofEntries(Map.entry("foo", 1), Map.entry("bar", 2)),
            Map.ofEntries(Map.entry("baz", 3), Map.entry("buzz", 4)),
            (name, doc, value) ->
                doc.append(
                    name,
                    new BsonDocument(
                        value.entrySet().stream()
                            .map(e -> new BsonElement(e.getKey(), new BsonInt32(e.getValue())))
                            .collect(Collectors.toList()))),
            name -> Field.builder(name).mapOf(Value.builder().intValue().required()));
    test.test();
  }

  @Test
  public void parsersEmptyMap() throws Exception {
    var field = Field.builder("foo").mapOf(Value.builder().intValue().required()).required();

    var doc = new BsonDocument("foo", new BsonDocument());

    try (var parser = BsonDocumentParser.fromRoot(doc).build()) {
      var value = parser.getField(field).unwrap();
      Assert.assertEquals(Map.of(), value);
    }
  }

  @Test
  public void testAsMapBuilder() throws Exception {
    var test =
        new FieldParseTestUtil<>(
            Map.ofEntries(Map.entry("foo", 1)),
            Map.ofEntries(Map.entry("bar", 2)),
            (name, doc, value) ->
                doc.append(
                    name,
                    new BsonDocument(
                        value.entrySet().stream()
                            .map(e -> new BsonElement(e.getKey(), new BsonInt32(e.getValue())))
                            .collect(Collectors.toList()))),
            name -> Field.builder(name).intField().asMap());
    test.test();
  }

  @Test
  public void mustNotBeEmptyThrowsForEmptyMap() throws Exception {
    var field =
        Field.builder("foo")
            .mapOf(Value.builder().stringValue().required())
            .mustNotBeEmpty()
            .required();

    var doc = new BsonDocument("foo", new BsonDocument());

    try (var parser = BsonDocumentParser.fromRoot(doc).build()) {
      Assert.assertThrows(BsonParseException.class, () -> parser.getField(field).unwrap());
    }
  }

  @Test
  public void mustNotContainEmptyStringAsKeyThrowsForEmptyString() throws Exception {
    var field =
        Field.builder("foo")
            .mapOf(Value.builder().stringValue().required())
            .mustNotContainEmptyStringAsKey()
            .required();

    var doc = new BsonDocument("foo", new BsonDocument("", new BsonString("bar")));

    try (var parser = BsonDocumentParser.fromRoot(doc).build()) {
      Assert.assertThrows(BsonParseException.class, () -> parser.getField(field).unwrap());
    }
  }

  @Test
  public void validatesKeys() throws Exception {
    var field =
        Field.builder("foo")
            .mapOf(Value.builder().intValue().required())
            .validateKeys(
                k ->
                    k.startsWith("good")
                        ? Optional.empty()
                        : Optional.of("must start with \"good\""))
            .required();

    var goodDoc =
        new BsonDocument(
            "foo",
            new BsonDocument()
                .append("good 1", new BsonInt32(1))
                .append("good 2", new BsonInt32(2)));
    try (var parser = BsonDocumentParser.fromRoot(goodDoc).build()) {
      // Shouldn't throw.
      parser.getField(field).unwrap();
    }

    var badDoc =
        new BsonDocument(
            "foo",
            new BsonDocument()
                .append("good 1", new BsonInt32(1))
                .append("bad 2", new BsonInt32(2)));
    try (var parser = BsonDocumentParser.fromRoot(badDoc).build()) {
      Assert.assertThrows(BsonParseException.class, () -> parser.getField(field).unwrap());
    }
  }

  @Test
  public void testEncodes() throws Exception {
    var test =
        new FieldEncodeTestUtil<>(
            Map.ofEntries(Map.entry("foo", "bar"), Map.entry("baz", "qux")),
            new BsonDocument()
                .append("foo", new BsonString("bar"))
                .append("baz", new BsonString("qux")),
            name -> Field.builder(name).stringField().asMap());
    test.test();
  }
}
