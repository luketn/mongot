package com.xgen.mongot.util;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.mongot.util.Check.checkArg;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Test;

public class CheckTest {

  @Test
  public void argIsPositive_zero_formatsCorrectMessage() {
    Exception e1 =
        assertThrows(IllegalArgumentException.class, () -> Check.argIsPositive(0, "num"));
    Exception e2 =
        assertThrows(IllegalArgumentException.class, () -> Check.argIsPositive(0.0, "num"));
    Exception e3 =
        assertThrows(
            IllegalArgumentException.class, () -> Check.argIsPositive(Duration.ofDays(0), "num"));

    assertThat(e1).hasMessageThat().isEqualTo("num must be positive but was 0");
    assertThat(e2).hasMessageThat().isEqualTo("num must be positive but was 0.000000");
    assertThat(e3).hasMessageThat().isEqualTo("num must be positive but was PT0S");
  }

  @Test
  public void argNotNegative_zero_formatsCorrectMessage() {
    Exception e1 =
        assertThrows(IllegalArgumentException.class, () -> Check.argNotNegative(-1, "num"));
    Exception e2 =
        assertThrows(IllegalArgumentException.class, () -> Check.argNotNegative(-1L, "num"));
    Exception e3 =
        assertThrows(IllegalArgumentException.class, () -> Check.argNotNegative(-.1, "num"));

    assertThat(e1).hasMessageThat().isEqualTo("num cannot be negative but was -1");
    assertThat(e2).hasMessageThat().isEqualTo("num cannot be negative but was -1");
    assertThat(e3).hasMessageThat().isEqualTo("num cannot be negative but was -0.100000");
  }

  @Test
  public void argNotNegative_zero_doesNotThrow() {
    Check.argNotNegative(0, "num");
    Check.argNotNegative(0L, "num");
    Check.argNotNegative(0.0, "num");
  }

  @Test
  public void testInstanceOfWhenConditionIsTrue() {
    Check.instanceOf(new ArrayList<>(), ArrayList.class);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInstanceOfWhenConditionIsFalse() {
    Check.instanceOf(new ArrayList<>(), LinkedList.class);
  }

  @Test
  public void testIsPresent() {
    String value = Check.isPresent(Optional.of("present"), "present");
    assertEquals("present", value);
  }

  @Test
  public void testIsPresentEmpty() {
    assertThrows(AssertionError.class, () -> Check.isPresent(Optional.empty(), "empty"));
  }

  @Test
  public void illegalFormatterDoesNotThrowException() {
    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> checkArg(false, "TEST:%d", 5));
    assertThat(ex).hasMessageThat().isEqualTo("TEST:%d [5]");
  }

  @Test
  public void formatArgsNull_checkArg_allowsNull() {
    IllegalArgumentException exceptionWithNull =
        assertThrows(
            IllegalArgumentException.class,
            () -> Check.checkArg(false, "TEST %s %s", "value", null));
    assertThat(exceptionWithNull).hasMessageThat().isEqualTo("TEST value null");

    IllegalArgumentException exceptionWithBothNull =
        assertThrows(
            IllegalArgumentException.class, () -> Check.checkArg(false, "TEST %s %s", null, null));
    assertThat(exceptionWithBothNull).hasMessageThat().isEqualTo("TEST null null");
  }

  @Test
  public void formatArgsNull_checkState_allowsNull() {
    IllegalStateException exceptionWithNull =
        assertThrows(
            IllegalStateException.class,
            () -> Check.checkState(false, "Expected %s but was %s", "value", null));
    assertThat(exceptionWithNull).hasMessageThat().isEqualTo("Expected value but was null");

    IllegalStateException exceptionWithBothNull =
        assertThrows(
            IllegalStateException.class,
            () -> Check.checkState(false, "Expected %s but was %s", null, null));
    assertThat(exceptionWithBothNull).hasMessageThat().isEqualTo("Expected null but was null");
  }

  @Test
  public void formatArgsNull_stateNotNull_allowsNull() {
    IllegalStateException exceptionWithNull =
        assertThrows(
            IllegalStateException.class,
            () -> Check.stateNotNull(null, "Expected %s but was %s", "value", null));
    assertThat(exceptionWithNull).hasMessageThat().isEqualTo("Expected value but was null");

    IllegalStateException exceptionWithBothNull =
        assertThrows(
            IllegalStateException.class,
            () -> Check.stateNotNull(null, "Expected %s but was %s", null, null));
    assertThat(exceptionWithBothNull).hasMessageThat().isEqualTo("Expected null but was null");
  }

  @Test
  public void expectedTypeMismatch() {
    assertThrows(
        UnsupportedOperationException.class,
        () -> Check.expectedType(TimeUnit.SECONDS, TimeUnit.HOURS));
    assertThrows(
        UnsupportedOperationException.class,
        () -> Check.expectedType((TimeUnit) null, TimeUnit.SECONDS));
    assertThrows(
        UnsupportedOperationException.class, () -> Check.expectedType(TimeUnit.SECONDS, null));
  }

  @Test
  public void expectedTypeMatches() {
    Check.expectedType(TimeUnit.SECONDS, TimeUnit.SECONDS);
  }

  @Test
  public void checkInstanceOfCastsToTheProvidedType() {
    String original = "hello";
    String result = Check.instanceOf(original, String.class);
    Assert.assertEquals(original, result);
  }

  @Test
  public void testHasSingleElementThrowsOnInvalidInput() {
    assertThrows(IllegalArgumentException.class, () -> Check.hasSingleElement(List.of(), "field"));
    assertThrows(
        IllegalArgumentException.class, () -> Check.hasSingleElement(List.of(1, 23, 94), "field"));
  }

  @Test
  public void testHasSingleElementReturnsValue() {
    Assert.assertEquals("elem", Check.hasSingleElement(List.of("elem"), "field"));
  }
}
