package com.xgen.mongot.util.bson.parser;

import org.bson.BsonDateTime;
import org.junit.Test;

public class BsonDateTimeFieldTest {
  @Test
  public void testParses() throws Exception {
    var test =
        new FieldParseTestUtil<>(
            new BsonDateTime(1L),
            new BsonDateTime(2L),
            (name, doc, value) -> doc.append(name, value),
            name -> Field.builder(name).bsonDateTimeField());
    test.test();
  }

  @Test
  public void testEncodes() throws Exception {
    var test =
        new FieldEncodeTestUtil<>(
            new BsonDateTime(1L),
            new BsonDateTime(1L),
            name -> Field.builder(name).bsonDateTimeField());
    test.test();
  }
}
