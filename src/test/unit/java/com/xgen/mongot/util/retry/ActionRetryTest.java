package com.xgen.mongot.util.retry;

import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.Callable;
import org.junit.Assert;
import org.junit.Test;

public class ActionRetryTest {

  private interface ObjectCallable extends Callable<Object> {}

  @Test(expected = IllegalStateException.class)
  public void shouldFailAfterExceedingMaxNumberOfAttempts() throws Exception {
    Callable<Object> callable =
        () -> {
          throw new IllegalStateException("Test Exception");
        };
    ActionRetry.onException(callable, IllegalStateException.class, 5);
  }

  @Test
  public void shouldReturnResultIfRetriesHelped() throws Exception {

    var expected = new Object();

    @SuppressWarnings("unchecked")
    Callable<Object> callable = mock(ObjectCallable.class);
    when(callable.call())
        .thenThrow(IllegalStateException.class)
        .thenThrow(IllegalStateException.class)
        .thenReturn(expected);

    Object actual = ActionRetry.onException(callable, IllegalStateException.class, 5);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void shouldThrowOnUnexpectedException() throws Exception {
    var expected = new Object();

    Callable<Object> callable = mock(ObjectCallable.class);
    when(callable.call())
        .thenThrow(IllegalStateException.class)
        .thenThrow(RuntimeException.class)
        .thenReturn(expected);

    assertThrows(
        RuntimeException.class,
        () -> ActionRetry.onException(callable, IllegalStateException.class, 5));
  }
}
