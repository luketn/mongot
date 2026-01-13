package com.xgen.mongot.replication.mongodb.steadystate.changestream;

import com.xgen.mongot.util.Check;
import java.util.List;
import java.util.Optional;

public class SteadyStateReplicationConfig {

  private final int numConcurrentChangeStreams;
  private final int changeStreamQueryMaxTimeMs;
  private final int changeStreamCursorMaxTimeSec;
  private final Optional<Boolean> enableChangeStreamProjection;
  private final int maxInFlightEmbeddingGetMores;
  private final Optional<Integer> embeddingGetMoreBatchSize;
  private final List<String> excludedChangestreamFields;
  private final boolean matchCollectionUuidForUpdateLookup;
  private final boolean enableSplitLargeChangeStreamEvents;

  private SteadyStateReplicationConfig(
      int numConcurrentChangeStreams,
      int changeStreamQueryMaxTimeMs,
      int changeStreamCursorMaxTimeSec,
      Optional<Boolean> enableChangeStreamProjection,
      int maxInFlightEmbeddingGetMores,
      Optional<Integer> embeddingGetMoreBatchSize,
      List<String> excludedChangestreamFields,
      boolean matchCollectionUuidForUpdateLookup,
      boolean enableSplitLargeChangeStreamEvents) {
    this.numConcurrentChangeStreams = numConcurrentChangeStreams;
    this.changeStreamQueryMaxTimeMs = changeStreamQueryMaxTimeMs;
    this.changeStreamCursorMaxTimeSec = changeStreamCursorMaxTimeSec;
    this.enableChangeStreamProjection = enableChangeStreamProjection;
    this.maxInFlightEmbeddingGetMores = maxInFlightEmbeddingGetMores;
    this.embeddingGetMoreBatchSize = embeddingGetMoreBatchSize;
    this.excludedChangestreamFields = excludedChangestreamFields;
    this.matchCollectionUuidForUpdateLookup = matchCollectionUuidForUpdateLookup;
    this.enableSplitLargeChangeStreamEvents = enableSplitLargeChangeStreamEvents;
  }

  public static SteadyStateReplicationConfig.Builder builder() {
    return new SteadyStateReplicationConfig.Builder();
  }

  public int getNumConcurrentChangeStreams() {
    return this.numConcurrentChangeStreams;
  }

  public int getChangeStreamQueryMaxTimeMs() {
    return this.changeStreamQueryMaxTimeMs;
  }

  public int getChangeStreamCursorMaxTimeSec() {
    return this.changeStreamCursorMaxTimeSec;
  }

  public Optional<Boolean> getEnableChangeStreamProjection() {
    return this.enableChangeStreamProjection;
  }

  public int getMaxInFlightEmbeddingGetMores() {
    return this.maxInFlightEmbeddingGetMores;
  }

  public Optional<Integer> getEmbeddingGetMoreBatchSize() {
    return this.embeddingGetMoreBatchSize;
  }

  public List<String> getExcludedChangestreamFields() {
    return this.excludedChangestreamFields;
  }

  public boolean getMatchCollectionUuidForUpdateLookup() {
    return this.matchCollectionUuidForUpdateLookup;
  }

  public boolean getEnableSplitLargeChangeStreamEvents() {
    return this.enableSplitLargeChangeStreamEvents;
  }

  public static class Builder {
    private Optional<Integer> numConcurrentChangeStreams;
    private Optional<Integer> changeStreamQueryMaxTimeMs;
    private Optional<Integer> changeStreamCursorMaxTimeSec;
    private Optional<Boolean> enableChangeStreamProjection;
    private Optional<Integer> maxInFlightEmbeddingGetMores;
    private Optional<Integer> embeddingGetMoreBatchSize;
    private List<String> excludedChangestreamFields;
    private boolean matchCollectionUuidForUpdateLookup;
    private boolean enableSplitLargeChangeStreamEvents;

    public Builder() {
      this.numConcurrentChangeStreams = Optional.empty();
      this.changeStreamQueryMaxTimeMs = Optional.empty();
      this.changeStreamCursorMaxTimeSec = Optional.empty();
      this.enableChangeStreamProjection = Optional.empty();
      this.maxInFlightEmbeddingGetMores = Optional.empty();
      this.embeddingGetMoreBatchSize = Optional.empty();
      this.excludedChangestreamFields = List.of();
      this.matchCollectionUuidForUpdateLookup = false;
      this.enableSplitLargeChangeStreamEvents = false;
    }

    public Builder setNumConcurrentChangeStreams(int value) {
      this.numConcurrentChangeStreams = Optional.of(value);
      return this;
    }

    public Builder setChangeStreamQueryMaxTimeMs(int value) {
      this.changeStreamQueryMaxTimeMs = Optional.of(value);
      return this;
    }

    public Builder setChangeStreamCursorMaxTimeSec(int value) {
      this.changeStreamCursorMaxTimeSec = Optional.of(value);
      return this;
    }

    public Builder setEnableChangeStreamProjection(Optional<Boolean> value) {
      this.enableChangeStreamProjection = value;
      return this;
    }

    public Builder setMaxInFlightEmbeddingGetMores(int value) {
      this.maxInFlightEmbeddingGetMores = Optional.of(value);
      return this;
    }

    public Builder setEmbeddingGetMoreBatchSize(Optional<Integer> value) {
      this.embeddingGetMoreBatchSize = value;
      return this;
    }

    public Builder setExcludedChangestreamFields(List<String> excludedChangestreamFields) {
      this.excludedChangestreamFields = excludedChangestreamFields;
      return this;
    }

    public Builder setMatchCollectionUuidForUpdateLookup(boolean value) {
      this.matchCollectionUuidForUpdateLookup = value;
      return this;
    }

    public Builder setEnableSplitLargeChangeStreamEvents(
        boolean enableSplitLargeChangeStreamEvents) {
      this.enableSplitLargeChangeStreamEvents = enableSplitLargeChangeStreamEvents;
      return this;
    }

    public SteadyStateReplicationConfig build() {
      return new SteadyStateReplicationConfig(
          Check.isPresent(this.numConcurrentChangeStreams, "numConcurrentChangeStreams"),
          Check.isPresent(this.changeStreamQueryMaxTimeMs, "changeStreamQueryMaxTimeMs"),
          Check.isPresent(this.changeStreamCursorMaxTimeSec, "changeStreamCursorMaxTimeSec"),
          this.enableChangeStreamProjection,
          Check.isPresent(this.maxInFlightEmbeddingGetMores, "maxInFlightEmbeddingGetMores"),
          this.embeddingGetMoreBatchSize,
          this.excludedChangestreamFields,
          this.matchCollectionUuidForUpdateLookup,
          this.enableSplitLargeChangeStreamEvents);
    }
  }
}
