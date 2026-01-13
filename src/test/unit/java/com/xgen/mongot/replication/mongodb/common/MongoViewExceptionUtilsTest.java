package com.xgen.mongot.replication.mongodb.common;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.truth.Truth;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoCursorNotFoundException;
import com.mongodb.MongoNodeIsRecoveringException;
import com.mongodb.MongoNotPrimaryException;
import com.mongodb.MongoQueryException;
import java.util.Optional;
import org.junit.Test;

public class MongoViewExceptionUtilsTest {

  @Test
  public void testIsViewPipelineRelatedReturnsTrueForMqe() {
    var exception = mock(MongoQueryException.class);
    Optional<Throwable> cause = Optional.of(exception);
    assertTrue(MongoViewExceptionUtils.isViewPipelineRelated(cause));
  }

  @Test
  public void testIsViewPipelineRelatedReturnsTrueForMce() {
    var exception = mock(MongoCommandException.class);
    Optional<Throwable> cause = Optional.of(exception);
    assertTrue(MongoViewExceptionUtils.isViewPipelineRelated(cause));
  }

  @Test
  public void testIsViewPipelineRelatedReturnsFalseForCursorNotFound() {
    var exception = mock(MongoCursorNotFoundException.class);
    Optional<Throwable> cause = Optional.of(exception);
    assertFalse(MongoViewExceptionUtils.isViewPipelineRelated(cause));
  }

  @Test
  public void testIsViewPipelineRelatedReturnsFalseForEmptyCause() {
    Optional<Throwable> cause = Optional.empty();
    assertFalse(MongoViewExceptionUtils.isViewPipelineRelated(cause));
  }

  @Test
  public void testIsViewPipelineRelatedReturnsFalseForRetryableMongoCommandErrors() {
    assertFalse(
        MongoViewExceptionUtils.isViewPipelineRelated(
            Optional.of(mock(MongoNodeIsRecoveringException.class))));
    assertFalse(
        MongoViewExceptionUtils.isViewPipelineRelated(
            Optional.of(mock(MongoNotPrimaryException.class))));
  }

  @Test
  public void testGetViewPipelineErrorMessageReturnsErrorMessage() {
    var exception = mock(MongoCommandException.class);
    when(exception.getErrorMessage()).thenReturn("Test error message");

    var result = MongoViewExceptionUtils.getViewPipelineErrorMessage(exception);
    Truth.assertThat(result).contains("Test error message");
  }

  @Test
  public void testGetViewPipelineErrorMessageWhenDriverErrorIsNull() {
    var exception = mock(MongoCommandException.class);
    when(exception.getErrorMessage()).thenReturn(null);

    var result = MongoViewExceptionUtils.getViewPipelineErrorMessage(exception);
    Truth.assertThat(result).contains("unknown error");
  }

  @Test
  public void testGetViewPipelineErrorMessageWhenDriverErrorIsBlank() {
    var exception = mock(MongoCommandException.class);
    when(exception.getErrorMessage()).thenReturn("   ");

    var result = MongoViewExceptionUtils.getViewPipelineErrorMessage(exception);
    Truth.assertThat(result).contains("unknown error");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetViewPipelineErrorMessageNullIsValidated() {
    MongoViewExceptionUtils.getViewPipelineErrorMessage(null);
  }
}
