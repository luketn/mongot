package com.xgen.mongot.cursor.batch;

import static com.xgen.mongot.util.Check.checkArg;

import java.util.Objects;
import java.util.Optional;

/**
 * Parent class for server-provided options on {@link
 * com.xgen.mongot.server.command.search.SearchCommand} and {@link
 * com.xgen.mongot.util.mongodb.GetMoreCommand}.
 *
 * <p>Whereas traditionally cursor options are only supplied on cursor establishment, Mongot allows
 * certain parameters to be updated per batch request.
 */
public class BatchCursorOptions {

  private static final BatchCursorOptions DEFAULT_OPTIONS =
      new BatchCursorOptions(Optional.empty(), Optional.empty());

  /**
   * docsRequested is deprecated, will be removed in a future release, batchSize will replace it.
   */
  @Deprecated(since = "MongoDB 8.1", forRemoval = true)
  protected final Optional<Integer> docsRequested;

  protected final Optional<Integer> batchSize;

  public BatchCursorOptions(Optional<Integer> docsRequested, Optional<Integer> batchSize) {
    checkArg(
        !(docsRequested.isPresent() && batchSize.isPresent()),
        "Specifying both 'docsRequested' and 'batchSize' is not allowed.");
    this.docsRequested = docsRequested;
    this.batchSize = batchSize;
  }

  /**
   * The number of results Mongod requests per batch. This field is optional and new values are
   * potentially provided each subsequent GetMore.
   */
  @Deprecated(since = "MongoDB 8.1", forRemoval = true)
  public Optional<Integer> getDocsRequested() {
    return this.docsRequested;
  }

  /**
   * batchSize: A strict maximum number of documents to return per batch. A batch may contain fewer
   * documents than the batchSize for various reasons, such as when not enough results are available
   * to complete a full batch.
   */
  public Optional<Integer> getBatchSize() {
    return this.batchSize;
  }

  /** The default cursor option values to use if none are provided by the server. */
  public static BatchCursorOptions empty() {
    return DEFAULT_OPTIONS;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BatchCursorOptions that = (BatchCursorOptions) o;
    return Objects.equals(this.docsRequested, that.docsRequested)
        && Objects.equals(this.batchSize, that.batchSize);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.docsRequested, this.batchSize);
  }
}
