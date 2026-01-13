package com.xgen.mongot.index.query;

import org.junit.Assert;
import org.junit.Test;

public class InvalidQueryExceptionTest {

  @Test
  public void testValidateConditional() throws Exception {
    InvalidQueryException.validate(true, "should not throw");
    Assert.assertThrows(
        InvalidQueryException.class,
        () -> InvalidQueryException.validate(false, "should throw exception"));
  }

  @Test
  public void testWrapIfThrowsWrapsException() throws Exception {
    Assert.assertThrows(InvalidQueryException.class, this::wrappedException);
  }

  @Test
  public void testWrapIfThrowsReturnsValue() throws Exception {
    Integer integer = InvalidQueryException.wrapIfThrows(() -> 5, MockCheckedException.class);
    Assert.assertEquals(Integer.valueOf(5), integer);
  }

  private void wrappedException() throws InvalidQueryException {
    InvalidQueryException.wrapIfThrows(
        () -> {
          throw new MockCheckedException();
        },
        MockCheckedException.class);
  }

  static class MockCheckedException extends Exception {}
}
