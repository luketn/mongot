package com.xgen.mongot.util.bson.parser;

import static com.xgen.mongot.util.bson.parser.Field.builder;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.lang3.Range;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.junit.Assert;
import org.junit.Test;

public class ListFieldTest {

  @Test
  public void parsesSingleValueList() throws Exception {
    var test =
        new FieldParseTestUtil<>(
            List.of("foo"),
            List.of("bar"),
            (name, doc, value) ->
                doc.append(
                    name,
                    new BsonArray(
                        value.stream().map(BsonString::new).collect(Collectors.toList()))),
            name -> builder(name).listOf(Value.builder().stringValue().required()));
    test.test();
  }

  @Test
  public void parsesMultiValueList() throws Exception {
    var test =
        new FieldParseTestUtil<>(
            List.of("foo", "bar"),
            List.of("baz", "buzz"),
            (name, doc, value) ->
                doc.append(
                    name,
                    new BsonArray(
                        value.stream().map(BsonString::new).collect(Collectors.toList()))),
            name -> builder(name).listOf(Value.builder().stringValue().required()));
    test.test();
  }

  @Test
  public void parsersEmptyList() throws Exception {
    var field =
        builder("foo").singleValueOrListOf(Value.builder().stringValue().required()).required();

    var doc = new BsonDocument("foo", new BsonArray());

    try (var parser = BsonDocumentParser.fromRoot(doc).build()) {
      var value = parser.getField(field).unwrap();
      Assert.assertEquals(List.of(), value);
    }
  }

  @Test
  public void testAsListBuilder() throws Exception {
    var test =
        new FieldParseTestUtil<>(
            List.of("foo"),
            List.of("bar"),
            (name, doc, value) ->
                doc.append(
                    name,
                    new BsonArray(
                        value.stream().map(BsonString::new).collect(Collectors.toList()))),
            name -> builder(name).stringField().asList());
    test.test();
  }

  @Test
  public void mustNotBeEmptyThrowsForEmptyList() throws Exception {
    var field =
        builder("foo").listOf(Value.builder().stringValue().required()).mustNotBeEmpty().required();

    var doc = new BsonDocument("foo", new BsonArray());

    try (var parser = BsonDocumentParser.fromRoot(doc).build()) {
      Assert.assertThrows(BsonParseException.class, () -> parser.getField(field).unwrap());
    }
  }

  @Test
  public void mustBeUniqueThrowsWithDuplicateEntries() throws Exception {
    var field =
        builder("foo").listOf(Value.builder().stringValue().required()).mustBeUnique().required();

    var doc =
        new BsonDocument(
            "foo", new BsonArray(List.of(new BsonString("bar"), new BsonString("bar"))));

    try (var parser = BsonDocumentParser.fromRoot(doc).build()) {
      Assert.assertThrows(BsonParseException.class, () -> parser.getField(field).unwrap());
    }
  }

  @Test
  public void listOfThrowsForSingleValue() throws Exception {
    var field = builder("foo").listOf(Value.builder().stringValue().required()).required();

    var doc = new BsonDocument("foo", new BsonString("bar"));

    try (var parser = BsonDocumentParser.fromRoot(doc).build()) {
      Assert.assertThrows(BsonParseException.class, () -> parser.getField(field).unwrap());
    }
  }

  @Test
  public void singleValueOrListOfParsesSingleValue() throws Exception {
    var field =
        builder("foo").singleValueOrListOf(Value.builder().stringValue().required()).required();

    var doc = new BsonDocument("foo", new BsonString("bar"));

    try (var parser = BsonDocumentParser.fromRoot(doc).build()) {
      var value = parser.getField(field).unwrap();
      Assert.assertEquals(List.of("bar"), value);
    }
  }

  @Test
  public void singleValueOrListOfParsesList() throws Exception {
    var field =
        builder("foo").singleValueOrListOf(Value.builder().stringValue().required()).required();

    var doc =
        new BsonDocument(
            "foo", new BsonArray(List.of(new BsonString("bar"), new BsonString("baz"))));

    try (var parser = BsonDocumentParser.fromRoot(doc).build()) {
      var value = parser.getField(field).unwrap();
      Assert.assertEquals(List.of("bar", "baz"), value);
    }
  }

  @Test
  public void testEncodesSingleValueAsList() {
    var test =
        new FieldEncodeTestUtil<>(
            List.of("bar"),
            new BsonArray(List.of(new BsonString("bar"))),
            name -> builder(name).stringField().asList());
    test.test();
  }

  @Test
  public void testEncodesMultipleValuesAsList() {
    var test =
        new FieldEncodeTestUtil<>(
            List.of("bar", "baz"),
            new BsonArray(List.of(new BsonString("bar"), new BsonString("baz"))),
            name -> builder(name).stringField().asList());
    test.test();
  }

  @Test
  public void testSingleValueOrListEncodesSingleValueAsValue() {
    var test =
        new FieldEncodeTestUtil<>(
            List.of("bar"),
            new BsonString("bar"),
            name -> builder(name).stringField().asSingleValueOrList());
    test.test();
  }

  @Test
  public void testSingleValueOrListEncodesMultipleValuesAsList() {
    var test =
        new FieldEncodeTestUtil<>(
            List.of("bar", "baz"),
            new BsonArray(List.of(new BsonString("bar"), new BsonString("baz"))),
            name -> builder(name).stringField().asSingleValueOrList());
    test.test();
  }

  @Test
  public void testMustHaveUniqueAttributesParsesValidEntries() throws Exception {
    var field =
        builder("foo")
            .listOf(Value.builder().stringValue().required())
            .mustHaveUniqueAttribute("name", Function.identity())
            .required();

    var doc =
        new BsonDocument(
            "foo", new BsonArray(List.of(new BsonString("bar"), new BsonString("baz"))));

    try (var parser = BsonDocumentParser.fromRoot(doc).build()) {
      var value = parser.getField(field).unwrap();
      Assert.assertEquals(List.of("bar", "baz"), value);
    }
  }

  @Test
  public void testMustHaveUniqueAttributesThrowsOnDuplicate() throws Exception {
    var field =
        builder("foo")
            .listOf(Value.builder().stringValue().required())
            .mustHaveUniqueAttribute("name", Function.identity())
            .required();

    var doc =
        new BsonDocument(
            "foo", new BsonArray(List.of(new BsonString("bar"), new BsonString("bar"))));

    try (var parser = BsonDocumentParser.fromRoot(doc).build()) {
      Assert.assertThrows(BsonParseException.class, () -> parser.getField(field).unwrap());
    }
  }

  @Test
  public void sizeMustBeWithinBoundsThrowsForOutOfBoundaries() throws Exception {
    var field =
        builder("foo")
            .listOf(Value.builder().intValue().required())
            .sizeMustBeWithinBounds(Range.of(3, 6))
            .required();

    var doc = new BsonDocument("foo", new BsonArray(List.of(new BsonInt32(1), new BsonInt32(2))));

    try (var parser = BsonDocumentParser.fromRoot(doc).build()) {
      Assert.assertThrows(BsonParseException.class, () -> parser.getField(field).unwrap());
    }
  }

  @Test
  public void sizeMustBeWithinBoundsDoesNotThrowWithinBoundaries() throws Exception {
    var field =
        builder("foo")
            .listOf(Value.builder().intValue().required())
            .sizeMustBeWithinBounds(Range.of(3, 6))
            .required();

    for (int size = 3; size <= 6; size++) {
      var doc =
          new BsonDocument(
              "foo",
              new BsonArray(
                  IntStream.range(0, size).mapToObj(BsonInt32::new).collect(Collectors.toList())));
      try (var parser = BsonDocumentParser.fromRoot(doc).build()) {
        var value = parser.getField(field).unwrap();
        Assert.assertEquals(size, value.size());
      }
    }
  }

  @Test
  public void testSkipInvalidElementsSkipsInvalidItems() throws BsonParseException {
    var field =
        builder("foo")
            .listOf(Value.builder().intValue().required())
            .skipInvalidElements()
            .required();

    var doc =
        new BsonDocument(
            "foo",
            new BsonArray(List.of(new BsonInt32(1), new BsonString("invalid"), new BsonInt32(3))));

    try (var parser = BsonDocumentParser.fromRoot(doc).build()) {
      List<Integer> value = parser.getField(field).unwrap();
      Assert.assertEquals(List.of(1, 3), value);
    }
  }

  @Test
  public void testSkipInvalidElementsSkipsForNestedList() throws BsonParseException {
    var field =
        builder("foo")
            .listOf(Value.builder().listOf(Value.builder().intValue().required()).required())
            .skipInvalidElements()
            .required();

    var doc =
        new BsonDocument(
            "foo",
            new BsonArray(
                List.of(
                    new BsonArray(List.of(new BsonInt32(3), new BsonInt32(4))),
                    new BsonArray(List.of(new BsonInt32(3), new BsonString("a"))),
                    new BsonInt32(5),
                    new BsonArray(
                        List.of(new BsonInt32(1), new BsonString("invalid"), new BsonInt32(3))))));

    try (var parser = BsonDocumentParser.fromRoot(doc).build()) {
      List<List<Integer>> value = parser.getField(field).unwrap();
      Assert.assertEquals(List.of(List.of(3, 4)), value);
    }
  }
}
