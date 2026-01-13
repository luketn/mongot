package com.xgen.mongot.util.bson.parser;

import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.Assert;
import org.junit.Test;

public class BsonDocumentBuilderTest {

  @Test
  public void testAddsRequired() {
    var field = Field.builder("foo").stringField().required();
    var doc = BsonDocumentBuilder.builder().field(field, "bar").build();
    var expected = new BsonDocument("foo", new BsonString("bar"));

    Assert.assertEquals(expected, doc);
  }

  @Test
  public void testAddsPresentOptional() {
    var field = Field.builder("foo").stringField().optional().noDefault();
    var doc = BsonDocumentBuilder.builder().field(field, Optional.of("bar")).build();
    var expected = new BsonDocument("foo", new BsonString("bar"));

    Assert.assertEquals(expected, doc);
  }

  @Test
  public void testDoesNotAddForEmptyOptional() {
    var field = Field.builder("foo").stringField().optional().noDefault();
    var doc = BsonDocumentBuilder.builder().field(field, Optional.empty()).build();
    var expected = new BsonDocument();

    Assert.assertEquals(expected, doc);
  }

  @Test
  public void testAddsWithDefault() {
    var field1 = Field.builder("foo").stringField().optional().withDefault("default");
    var field2 = Field.builder("bar").stringField().optional().withDefault("default");
    var doc =
        BsonDocumentBuilder.builder().field(field1, "default").field(field2, "non-default").build();
    var expected =
        new BsonDocument()
            .append("foo", new BsonString("default"))
            .append("bar", new BsonString("non-default"));

    Assert.assertEquals(expected, doc);
  }

  @Test
  public void testAddsWithDefaultOmitDefaultValue() {
    var field1 = Field.builder("foo").stringField().optional().withDefault("default");
    var field2 = Field.builder("bar").stringField().optional().withDefault("default");
    var doc =
        BsonDocumentBuilder.builder()
            .fieldOmitDefaultValue(field1, "default")
            .fieldOmitDefaultValue(field2, "non-default")
            .build();
    var expected = new BsonDocument("bar", new BsonString("non-default"));

    Assert.assertEquals(expected, doc);
  }
}
