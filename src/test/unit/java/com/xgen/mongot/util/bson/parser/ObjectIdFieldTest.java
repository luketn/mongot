package com.xgen.mongot.util.bson.parser;

import org.bson.BsonDocument;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;

public class ObjectIdFieldTest {

  @Test
  public void testParsesObjectId() throws Exception {
    var test =
        new FieldParseTestUtil<>(
            new ObjectId(),
            new ObjectId(),
            (name, doc, value) -> doc.append(name, new BsonObjectId(value)),
            name -> Field.builder(name).objectIdField());
    test.test();
  }

  @Test
  public void testParsesString() throws Exception {
    var test =
        new FieldParseTestUtil<>(
            new ObjectId(),
            new ObjectId(),
            (name, doc, value) -> doc.append(name, new BsonString(value.toHexString())),
            name -> Field.builder(name).objectIdField());
    test.test();
  }

  @Test
  public void throwsExceptionIfInvalidStringFormat() throws Exception {
    var field = Field.builder("foo").objectIdField().encodeAsString().required();
    var doc = new BsonDocument("foo", new BsonString("not-an-objectid"));

    try (var parser = BsonDocumentParser.fromRoot(doc).build()) {
      Assert.assertThrows(BsonParseException.class, () -> parser.getField(field));
    }
  }

  @Test
  public void testEncodesAsObjectId() throws Exception {
    var objectId = new ObjectId();
    var test =
        new FieldEncodeTestUtil<>(
            objectId, new BsonObjectId(objectId), name -> Field.builder(name).objectIdField());
    test.test();
  }

  @Test
  public void testEncodesAsString() throws Exception {
    var objectId = new ObjectId("507f191e810c19729de860ea");
    var test =
        new FieldEncodeTestUtil<>(
            objectId,
            new BsonString("507f191e810c19729de860ea"),
            name -> Field.builder(name).objectIdField().encodeAsString());
    test.test();
  }
}
