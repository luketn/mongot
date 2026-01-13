package com.xgen.mongot.replication.mongodb.synonyms;

import static com.xgen.mongot.util.Check.checkState;

import java.util.Objects;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;

/**
 * The "high-water" mark for a {@link com.xgen.mongot.index.synonym.SynonymMapping}.
 *
 * <p>For a valid {@link com.xgen.mongot.index.synonym.SynonymMapping}, represents either the
 * operationTime of the last successful sync, or the postBatchResumeToken of an empty change stream
 * cursor watching the synonym source collection namespace. Said differently, a time at which a
 * {@link com.xgen.mongot.index.synonym.SynonymMapping} is known to be up-to-date.
 *
 * <p>For an invalid {@link com.xgen.mongot.index.synonym.SynonymMapping}, represents the
 * operationTime of the last collection scan that yielded an invalid {@link
 * com.xgen.mongot.index.synonym.SynonymDocument}. This is a time at which a collection is known to
 * have an invalid document - a change event after this time could signal that the collection has
 * become valid.
 */
public class SynonymMappingHighWaterMark {
  private final Optional<BsonTimestamp> operationTime;
  private final Optional<BsonDocument> resumeToken;

  private SynonymMappingHighWaterMark(
      Optional<BsonTimestamp> operationTime, Optional<BsonDocument> resumeToken) {
    checkState(
        operationTime.isEmpty() || resumeToken.isEmpty(),
        "only one of operation time and resume token can be specified");

    this.operationTime = operationTime;
    this.resumeToken = resumeToken;
  }

  static SynonymMappingHighWaterMark create(BsonTimestamp operationTime) {
    return new SynonymMappingHighWaterMark(Optional.of(operationTime), Optional.empty());
  }

  static SynonymMappingHighWaterMark create(BsonDocument resumeToken) {
    return new SynonymMappingHighWaterMark(Optional.empty(), Optional.of(resumeToken));
  }

  static SynonymMappingHighWaterMark createEmpty() {
    return new SynonymMappingHighWaterMark(Optional.empty(), Optional.empty());
  }

  public Optional<BsonTimestamp> getOperationTime() {
    return this.operationTime;
  }

  public Optional<BsonDocument> getResumeToken() {
    return this.resumeToken;
  }

  public boolean isPresent() {
    return this.operationTime.isPresent() || this.resumeToken.isPresent();
  }

  @Override
  public String toString() {
    return String.format(
        "SynonymMappingHighWaterMark{operationTime=%s, resumeToken=%s}",
        this.operationTime.toString(), this.resumeToken.toString());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SynonymMappingHighWaterMark that = (SynonymMappingHighWaterMark) o;
    return this.operationTime.equals(that.operationTime)
        && this.resumeToken.equals(that.resumeToken);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.operationTime, this.resumeToken);
  }
}
