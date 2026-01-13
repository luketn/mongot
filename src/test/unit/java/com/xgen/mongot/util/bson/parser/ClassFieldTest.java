package com.xgen.mongot.util.bson.parser;

import java.util.Objects;
import java.util.Optional;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.Assert;
import org.junit.Test;

public class ClassFieldTest {

  public static class Foo implements DocumentEncodable {

    public static class Fields {

      public static final Field.Required<Boolean> BAR =
          Field.builder("bar").booleanField().required();
    }

    private final boolean bar;

    public Foo(boolean bar) {
      this.bar = bar;
    }

    static Foo fromBson(DocumentParser parser) throws BsonParseException {
      var bar = parser.getField(Fields.BAR).unwrap();

      return new Foo(bar);
    }

    @Override
    public BsonDocument toBson() {
      return BsonDocumentBuilder.builder().field(Fields.BAR, this.bar).build();
    }

    public boolean isBar() {
      return this.bar;
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof Foo && ((Foo) other).bar == this.bar;
    }

    @Override
    public int hashCode() {
      return Objects.hash(this.bar);
    }
  }

  @Test
  public void testParsesFromDocument() throws Exception {
    var test =
        new FieldParseTestUtil<>(
            new Foo(true),
            new Foo(false),
            (name, doc, value) ->
                doc.append(name, new BsonDocument("bar", new BsonBoolean(value.bar))),
            name -> Field.builder(name).classField(Foo::fromBson).disallowUnknownFields());
    test.test();
  }

  @Test
  public void testParsesFromDocumentAllowUnknownFields() throws Exception {
    BsonDocument doc =
        new BsonDocument()
            .append(
                "outer",
                new BsonDocument()
                    .append("bar", new BsonBoolean(true))
                    .append("unknown", new BsonString("field")));

    Field.Required<Foo> field =
        Field.builder("outer").classField(Foo::fromBson).allowUnknownFields().required();

    try (var parser = BsonDocumentParser.fromRoot(doc).build()) {
      var value = parser.getField(field);
      Assert.assertEquals(new Foo(true), value.unwrap());
    }
  }

  @Test
  public void testDoesNotParseFromDocumentDisallowUnknownFields() throws Exception {
    BsonDocument doc =
        new BsonDocument()
            .append(
                "outer",
                new BsonDocument()
                    .append("bar", new BsonBoolean(true))
                    .append("unknown", new BsonString("field")));

    Field.Required<Foo> field =
        Field.builder("outer").classField(Foo::fromBson).disallowUnknownFields().required();

    try (var parser = BsonDocumentParser.fromRoot(doc).build()) {
      Assert.assertThrows(BsonParseException.class, () -> parser.getField(field));
    }
  }

  @Test
  public void testParsesFromValue() throws Exception {
    var test =
        new FieldParseTestUtil<>(
            "foo",
            "bar",
            (name, doc, value) -> doc.append(name, new BsonString(value)),
            name ->
                Field.builder(name)
                    .classField((context, value) -> value.asString().getValue(), BsonString::new));

    test.test();
  }

  @Test
  public void testParsesFromValueAsDocumentAllowUnknownFields() throws Exception {
    BsonDocument doc =
        new BsonDocument()
            .append("bar", new BsonBoolean(true))
            .append("unknown", new BsonString("field"));

    Value.Required<Foo> value =
        Value.builder().classValue(Foo::fromBson).allowUnknownFields().required();

    Foo foo = value.getParser().parse(BsonParseContext.root(), doc);
    Assert.assertEquals(new Foo(true), foo);
  }

  @Test
  public void testDoesNotParseFromValueAsDocumentDisallowUnknownFields() throws Exception {
    BsonDocument doc =
        new BsonDocument()
            .append("bar", new BsonBoolean(true))
            .append("unknown", new BsonString("field"));

    Value.Required<Foo> value =
        Value.builder().classValue(Foo::fromBson).disallowUnknownFields().required();

    Assert.assertThrows(
        BsonParseException.class, () -> value.getParser().parse(BsonParseContext.root(), doc));
  }

  @Test
  public void supportsArbitraryValidation() throws Exception {
    var field =
        Field.builder("foo")
            .classField(Foo::fromBson)
            .disallowUnknownFields()
            .validate(foo -> foo.bar ? Optional.of("cannot have bar as true") : Optional.empty())
            .required();

    var barFalse = new BsonDocument("foo", new BsonDocument("bar", new BsonBoolean(false)));
    try (var parser = BsonDocumentParser.fromRoot(barFalse).build()) {
      var foo = parser.getField(field).unwrap();
      Assert.assertEquals(new Foo(false), foo);
    }

    var barTrue = new BsonDocument("foo", new BsonDocument("bar", new BsonBoolean(true)));
    try (var parser = BsonDocumentParser.fromRoot(barTrue).build()) {
      Assert.assertThrows(BsonParseException.class, () -> parser.getField(field));
    }
  }

  @Test
  public void testEncodesFromEncodable() throws Exception {
    var test =
        new FieldEncodeTestUtil<>(
            new Foo(true),
            new BsonDocument("bar", new BsonBoolean(true)),
            name -> Field.builder(name).classField(Foo::fromBson).disallowUnknownFields());
    test.test();
  }

  @Test
  public void testEncodesFromExplicitToBson() throws Exception {
    var test =
        new FieldEncodeTestUtil<>(
            new Foo(true),
            new BsonDocument("bar", new BsonBoolean(true)),
            name ->
                Field.builder(name).classField(Foo::fromBson, Foo::toBson).disallowUnknownFields());
    test.test();
  }
}
