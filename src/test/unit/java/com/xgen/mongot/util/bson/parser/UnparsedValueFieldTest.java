package com.xgen.mongot.util.bson.parser;

import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonObjectId;
import org.junit.Test;

public class UnparsedValueFieldTest {
  @Test
  public void testParses() throws Exception {
    var test =
        new FieldParseTestUtil<>(
            new BsonObjectId(),
            new BsonBoolean(false),
            (name, doc, value) -> doc.append(name, value),
            name -> Field.builder(name).unparsedValueField());
    test.test();
  }

  @Test
  public void testEncodes() throws Exception {
    var test =
        new FieldEncodeTestUtil<>(
            new BsonDocument("a", new BsonBoolean(true)),
            new BsonDocument("a", new BsonBoolean(true)),
            name -> Field.builder(name).unparsedValueField());
    test.test();
  }
}
