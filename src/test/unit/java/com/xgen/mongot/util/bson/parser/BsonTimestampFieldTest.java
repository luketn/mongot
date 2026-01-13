package com.xgen.mongot.util.bson.parser;

import org.bson.BsonTimestamp;
import org.junit.Test;

public class BsonTimestampFieldTest {
  @Test
  public void testParses() throws Exception {
    var test =
        new FieldParseTestUtil<>(
            new BsonTimestamp(1),
            new BsonTimestamp(10, 1),
            (name, doc, value) -> doc.append(name, value),
            name -> Field.builder(name).bsonTimestampField());
    test.test();
  }

  @Test
  public void testEncodes() {
    var test =
        new FieldEncodeTestUtil<>(
            new BsonTimestamp(10L),
            new BsonTimestamp(10L),
            name -> Field.builder(name).bsonTimestampField());
    test.test();
  }
}
