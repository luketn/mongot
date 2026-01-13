package com.xgen.mongot.util.bson.parser;

import com.xgen.mongot.util.FieldPath;
import java.util.Optional;
import org.junit.Assert;
import org.junit.Test;

public class BsonParseExceptionTest {

  @Test
  public void testMessageWithNoPath() {
    var exception = new BsonParseException("my message", Optional.empty());
    Assert.assertEquals("my message", exception.getMessage());
  }

  @Test
  public void testMessageAtRoot() {
    var exception = new BsonParseException("must be true", Optional.of(FieldPath.parse("a")));
    Assert.assertEquals("\"a\" must be true", exception.getMessage());
  }

  @Test
  public void testMessageWithDeepPath() {
    var exception = new BsonParseException("must be true", Optional.of(FieldPath.parse("a.b.c")));
    Assert.assertEquals("\"a.b.c\" must be true", exception.getMessage());
  }
}
