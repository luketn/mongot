package com.xgen.mongot.util.bson.parser;

import org.bson.BsonBoolean;
import org.junit.Test;

public class BooleanFieldTest {

  @Test
  public void testParses() throws Exception {
    var test =
        new FieldParseTestUtil<>(
            true,
            false,
            (name, doc, value) -> doc.append(name, new BsonBoolean(value)),
            name -> Field.builder(name).booleanField());
    test.test();
  }

  @Test
  public void testEncodes() throws Exception {
    var test =
        new FieldEncodeTestUtil<>(
            true, new BsonBoolean(true), name -> Field.builder(name).booleanField());
    test.test();
  }
}
