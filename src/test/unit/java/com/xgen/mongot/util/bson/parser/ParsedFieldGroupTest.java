package com.xgen.mongot.util.bson.parser;

import java.util.Optional;
import org.junit.Assert;
import org.junit.Test;

public class ParsedFieldGroupTest {

  private static final ParsedField.Optional<Integer> PRESENT_1 =
      new ParsedField.Optional<>("present1", Optional.of(1));

  private static final ParsedField.Optional<Integer> PRESENT_2 =
      new ParsedField.Optional<>("present2", Optional.of(2));

  private static final ParsedField.Optional<Integer> EMPTY_1 =
      new ParsedField.Optional<>("empty1", Optional.empty());

  private static final ParsedField.Optional<Integer> EMPTY_2 =
      new ParsedField.Optional<>("empty2", Optional.empty());

  @Test
  public void testExactlyOneOf() throws Exception {
    var group = new ParsedFieldGroup(BsonParseContext.root());

    Assert.assertEquals(1, (int) group.exactlyOneOf(PRESENT_1, EMPTY_1));
    Assert.assertEquals(1, (int) group.exactlyOneOf(EMPTY_1, EMPTY_2, PRESENT_1));
    Assert.assertThrows(BsonParseException.class, () -> group.exactlyOneOf(EMPTY_1, EMPTY_2));
    Assert.assertThrows(BsonParseException.class, () -> group.exactlyOneOf(PRESENT_1, PRESENT_2));
  }

  @Test
  public void testAtMostOneOf() throws Exception {
    var group = new ParsedFieldGroup(BsonParseContext.root());

    Assert.assertEquals(Optional.of(1), group.atMostOneOf(PRESENT_1, EMPTY_1));
    Assert.assertEquals(Optional.of(1), group.atMostOneOf(EMPTY_1, EMPTY_2, PRESENT_1));
    Assert.assertEquals(Optional.empty(), group.atMostOneOf(EMPTY_1, EMPTY_2));
    Assert.assertThrows(BsonParseException.class, () -> group.atMostOneOf(PRESENT_1, PRESENT_2));
  }

  @Test
  public void testAtLeastOneOf() throws Exception {
    var group = new ParsedFieldGroup(BsonParseContext.root());

    // These should not throw.
    group.atLeastOneOf(PRESENT_1, EMPTY_1);
    group.atLeastOneOf(PRESENT_1, PRESENT_2);

    Assert.assertThrows(BsonParseException.class, () -> group.atLeastOneOf(EMPTY_1, EMPTY_2));
  }
}
