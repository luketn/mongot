package com.xgen.mongot.util;

import com.xgen.mongot.util.functionalinterfaces.CheckedBiExceptionFunction;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.Assert;
import org.junit.Test;

public class CheckedStreamTest {

  @Test(expected = MockException.class)
  public void testCollectToMapCheckedBiValueException() throws MockException, MockException2 {

    CheckedBiExceptionFunction<String, Void, MockException, MockException2> function =
        item -> {
          if (item.equals("one")) {
            throw new MockException();
          } else {
            throw new MockException2();
          }
        };

    CheckedStream.from(Stream.of("one"))
        .collectToMapCheckedBiValueException(item -> item, function);
  }

  @Test(expected = MockException.class)
  public void testMapAndCollectCheckedBiValueException() throws MockException, MockException2 {

    CheckedBiExceptionFunction<String, Void, MockException, MockException2> function =
        item -> {
          if (item.equals("one")) {
            throw new MockException();
          } else {
            throw new MockException2();
          }
        };

    CheckedStream.from(Stream.of("one")).mapAndCollectCheckedBiValueException(function);
  }

  @Test
  public void testMapAndCollectCheckedWithoutException() {
    List<Integer> result = CheckedStream.from(Stream.of(1, 2)).mapAndCollectChecked(x -> x * 2);
    Assert.assertEquals(List.of(2, 4), result);
  }

  @Test(expected = MockException.class)
  public void testMapAndCollectCheckedExceptionIsPropagated() throws Exception {
    CheckedStream.from(Stream.of(1))
        .mapAndCollectChecked(
            x -> {
              throw new MockException();
            });
  }

  @Test
  public void testCollectToMapWithoutException() {
    Map<String, String> result =
        CheckedStream.from(Stream.of("one", "two"))
            .collectToMapChecked(item -> "key:" + item, item -> "value:" + item);
    Assert.assertEquals(Map.of("key:one", "value:one", "key:two", "value:two"), result);
  }

  @Test(expected = IllegalStateException.class)
  public void testCollectToMapDuplicateKey() {
    CheckedStream.from(Stream.of("one", "two"))
        .collectToMapChecked(item -> "key", item -> "value:" + item);
  }

  @Test(expected = MockException.class)
  public void testCollectToMapExceptionPropagatedFromKeyFn() throws MockException {
    CheckedStream.from(Stream.of("one"))
        .collectToMapChecked(
            item -> {
              throw new MockException();
            },
            item -> item);
  }

  @Test(expected = MockException.class)
  public void testCollectToMapExceptionPropagatedFromValueFn() throws MockException {
    CheckedStream.from(Stream.of("one"))
        .collectToMapChecked(
            item -> item,
            item -> {
              throw new MockException();
            });
  }

  private static class MockException extends Exception {}

  private static class MockException2 extends Exception {}
}
