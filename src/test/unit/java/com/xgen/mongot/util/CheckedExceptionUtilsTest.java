package com.xgen.mongot.util;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

import java.io.IOException;
import org.junit.Test;

public class CheckedExceptionUtilsTest {

  public static final String MSG = "Error message 1";

  @Test
  public void propagateCheckedIfType1_expectedTypePropagated() {
    Exception cause = new IOException(MSG);
    RuntimeException re = new RuntimeException(cause);

    try {
      CheckedExceptionUtils.propagateCheckedIfType(re, IOException.class);
    } catch (IOException e) {
      // KEEP: Explicit catch blocks used here to test that there's need for explicit checked
      // exception handling
      assertEquals(MSG, e.getMessage());
    } catch (Exception e) {
      fail("Unexpected exception: " + e);
    }
  }

  @Test
  public void propagateCheckedIfType1_runtimeExceptionPropagatedWhenNotExpectedType() {
    Exception cause = new IOException(MSG);
    RuntimeException re = new RuntimeException(cause);

    try {
      CheckedExceptionUtils.propagateCheckedIfType(re, CloneNotSupportedException.class);
    } catch (CloneNotSupportedException e) {
      // KEEP: Explicit catch blocks used here to test that there's need for explicit checked
      // exception handling
      fail("Unexpected exception: " + e);
    } catch (RuntimeException e) {
      assertNotEquals(MSG, e.getMessage());
      assertThat(e.getMessage()).contains(MSG);
    } catch (Exception e) {
      fail("Unexpected exception: " + e);
    }
  }

  @Test
  public void propagateCheckedIfType1_nullCause() {
    Exception cause = null;
    RuntimeException re = new RuntimeException(cause);

    try {
      CheckedExceptionUtils.propagateCheckedIfType(re, IOException.class);
    } catch (RuntimeException e) {
      assertThat(true).isTrue();
    } catch (IOException e) {
      // KEEP: Explicit catch blocks used here to test that there's need for explicit checked
      // exception handling
      fail("Unexpected exception: " + e);
    } catch (Exception e) {
      fail("Unexpected exception: " + e);
    }
  }

  @Test
  public void propagateCheckedIfType2_expectedTypePropagated() {
    Exception cause = new IOException(MSG);
    RuntimeException re = new RuntimeException(cause);

    try {
      CheckedExceptionUtils.propagateCheckedIfType(
          re, IOException.class, CloneNotSupportedException.class);
    } catch (IOException e) {
      // KEEP: Explicit catch blocks used here to test that there's need for explicit checked
      // exception handling
      assertEquals(MSG, e.getMessage());
    } catch (CloneNotSupportedException e) {
      // KEEP: Explicit catch blocks used here to test that there's need for explicit checked
      // exception handling
      fail("Unexpected exception: " + e);
    } catch (Exception e) {
      fail("Unexpected exception: " + e);
    }
  }

  @Test
  public void propagateCheckedIfType2_runtimeExceptionPropagatedWhenNotExpectedType() {
    Exception cause = new ClassNotFoundException(MSG);
    RuntimeException re = new RuntimeException(cause);

    try {
      CheckedExceptionUtils.propagateCheckedIfType(
          re, IOException.class, CloneNotSupportedException.class);
    } catch (IOException | CloneNotSupportedException e) {
      // KEEP: Explicit catch blocks used here to test that there's need for explicit checked
      // exception handling
      fail("Unexpected exception: " + e);
    } catch (RuntimeException e) {
      assertNotEquals(MSG, e.getMessage());
      assertThat(e.getMessage()).contains(MSG);
    } catch (Exception e) {
      fail("Unexpected exception: " + e);
    }
  }

  @Test
  public void propagateCheckedIfType2_originalExceptionPropagatedWhenNullCause() {
    Exception cause = null;
    RuntimeException re = new RuntimeException(MSG, cause);

    try {
      CheckedExceptionUtils.propagateCheckedIfType(
          re, IOException.class, CloneNotSupportedException.class);
    } catch (RuntimeException e) {
      assertEquals(MSG, e.getMessage());
    } catch (IOException | CloneNotSupportedException e) {
      // KEEP: Explicit catch blocks used here to test that there's need for explicit checked
      // exception handling
      fail("Unexpected exception: " + e);
    } catch (Exception e) {
      fail("Unexpected exception: " + e);
    }
  }

  @Test
  public void propagateUnwrappedIfTypeElseRuntime_exception1Propagated() {
    IOException cause = new IOException(MSG);
    Throwable throwable = new Exception(cause);

    try {
      CheckedExceptionUtils.propagateUnwrappedIfTypeElseRuntime(
          throwable, IOException.class, CloneNotSupportedException.class);
    } catch (RuntimeException | CloneNotSupportedException e) {
      // KEEP: Explicit catch blocks used here to test that there's need for explicit checked
      // exception handling
      fail("Unexpected exception: " + e);
    } catch (IOException e) {
      // KEEP: Explicit catch blocks used here to test that there's need for explicit checked
      // exception handling
      assertEquals(MSG, e.getMessage());
    } catch (Exception e) {
      fail("Unexpected exception: " + e);
    }
  }

  @Test
  public void propagateUnwrappedIfTypeElseRuntime_exception2Propagated() {
    CloneNotSupportedException cause = new CloneNotSupportedException(MSG);
    Throwable throwable = new Exception(cause);

    try {
      CheckedExceptionUtils.propagateUnwrappedIfTypeElseRuntime(
          throwable, IOException.class, CloneNotSupportedException.class);
    } catch (RuntimeException | IOException e) {
      // KEEP: Explicit catch blocks used here to test that there's need for explicit checked
      // exception handling
      fail("Unexpected exception: " + e);
    } catch (CloneNotSupportedException e) {
      // KEEP: Explicit catch blocks used here to test that there's need for explicit checked
      // exception handling
      assertEquals(MSG, e.getMessage());
    } catch (Exception e) {
      fail("Unexpected exception: " + e);
    }
  }

  @Test
  public void propagateUnwrappedIfTypeElseRuntime_runtimeExceptionPropagatedWhenNotExpectedType() {
    ClassNotFoundException cause = new ClassNotFoundException(MSG);
    Throwable throwable = new Exception(cause);

    try {
      CheckedExceptionUtils.propagateUnwrappedIfTypeElseRuntime(
          throwable, IOException.class, CloneNotSupportedException.class);
    } catch (RuntimeException e) {
      // KEEP: Explicit catch blocks used here to test that there's need for explicit checked
      // exception handling
      assertNotEquals(MSG, e.getMessage());
      assertThat(e.getMessage()).contains(MSG);
      assertSame(throwable, e.getCause());
    } catch (IOException | CloneNotSupportedException e) {
      // KEEP: Explicit catch blocks used here to test that there's need for explicit checked
      // exception handling
      fail("Unexpected exception: " + e);
    } catch (Exception e) {
      fail("Unexpected exception: " + e);
    }
  }

  @Test
  public void propagateUnwrappedIfTypeElseRuntime_runtimeExceptionPropagatedWhenNullCause() {
    ClassNotFoundException cause = null;
    Throwable throwable = new Exception(MSG, cause);

    try {
      CheckedExceptionUtils.propagateUnwrappedIfTypeElseRuntime(
          throwable, IOException.class, CloneNotSupportedException.class);
    } catch (RuntimeException e) {
      // KEEP: Explicit catch blocks used here to test that there's need for explicit checked
      // exception handling
      assertNotEquals(MSG, e.getMessage());
      assertThat(e.getMessage()).contains(MSG);
      assertSame(throwable, e.getCause());
    } catch (IOException | CloneNotSupportedException e) {
      // KEEP: Explicit catch blocks used here to test that there's need for explicit checked
      // exception handling
      fail("Unexpected exception: " + e);
    } catch (Exception e) {
      fail("Unexpected exception: " + e);
    }
  }
}
