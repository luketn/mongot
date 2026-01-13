package com.xgen.testing.mongot.mock.cursor;

import com.xgen.mongot.cursor.CursorConfig;
import com.xgen.mongot.cursor.serialization.MongotCursorBatch;
import com.xgen.mongot.cursor.serialization.MongotCursorResult;
import com.xgen.mongot.cursor.serialization.MongotIntermediateCursorBatch;
import com.xgen.mongot.index.CountMetaBatchProducer;
import com.xgen.mongot.index.Variables;
import com.xgen.mongot.index.lucene.explain.information.SearchExplainInformation;
import com.xgen.testing.mongot.index.MetaResultsBuilder;
import com.xgen.testing.mongot.mock.index.SearchResultBatch;
import com.xgen.testing.mongot.mock.index.VectorSearchResultBatch;
import java.util.Optional;
import org.bson.BsonValue;

public class MongotCursorBatches {

  public static final long MOCK_SEARCH_CURSOR_ID = 1;
  public static final long MOCK_META_CURSOR_ID = 2;

  public static final int MOCK_CURSOR_BATCH_SIZE = 4;

  /** Returns a MongotCursorBatch with a lowerBound count of 1000 for use in tests. */
  public static MongotCursorBatch mockInitialMongotCursorBatch() {
    return mockInitialMongotCursorBatch(
        MOCK_SEARCH_CURSOR_ID, new SearchResultBatch(MOCK_CURSOR_BATCH_SIZE), "foo.bar", 1000);
  }

  /** Returns a MongotCursorBatch with the specified lowerBoundCount for use in tests. */
  public static MongotCursorBatch mockInitialMongotCursorBatch(
      long cursorId, SearchResultBatch resultBatch, String namespace, long lowerBoundCount) {
    return new MongotCursorBatch(
        new MongotCursorResult(cursorId, resultBatch.getBsonResults(), namespace, Optional.empty()),
        Optional.empty(),
        Optional.of(
            new Variables(MetaResultsBuilder.mockLowerBoundMetaResults(lowerBoundCount)).toBson()));
  }

  /** Returns a MongotCursorBatch for vector search for use in tests. */
  public static MongotCursorBatch mockInitialMongotCursorBatchForVectorSearch() {
    return mockInitialMongotCursorBatchForVectorSearch(
        MOCK_SEARCH_CURSOR_ID, new VectorSearchResultBatch(MOCK_CURSOR_BATCH_SIZE), "foo.bar");
  }

  /** Returns a MongotCursorBatch for vector search for use in tests. */
  public static MongotCursorBatch mockInitialMongotCursorBatchForVectorSearch(
      long cursorId, VectorSearchResultBatch resultBatch, String namespace) {
    return new MongotCursorBatch(
        new MongotCursorResult(cursorId, resultBatch.getBsonResults(), namespace, Optional.empty()),
        Optional.empty(),
        Optional.empty());
  }

  /** Returns a MongotCursorBatch that can be used in tests that require one. */
  public static MongotCursorBatch mockMongotCursorBatch() {
    return mockMongotCursorBatch(
        MOCK_SEARCH_CURSOR_ID,
        new SearchResultBatch(MOCK_CURSOR_BATCH_SIZE).getBsonResults(),
        "foo.bar");
  }

  /** Returns a MongotCursorBatch that can be used in tests that require one. */
  public static MongotCursorBatch mockMongotCursorBatch(
      long cursorId, BsonValue resultBatch, String namespace) {
    return mockMongotCursorBatch(
        cursorId, resultBatch, Optional.empty(), namespace, Optional.empty());
  }

  /** Returns a MongotCursorBatch that can be used in tests that require one. */
  public static MongotCursorBatch mockMongotCursorBatch(
      long cursorId,
      BsonValue resultBatch,
      Optional<SearchExplainInformation> explain,
      String namespace,
      Optional<MongotCursorResult.Type> type) {
    return new MongotCursorBatch(
        new MongotCursorResult(cursorId, resultBatch, namespace, type), explain, Optional.empty());
  }

  /** Returns a MongotCursorBatch for vector search that can be used in tests that require one. */
  public static MongotCursorBatch mockMongotCursorBatchForVectorSearch() {
    return mockMongotCursorBatch(
        MOCK_SEARCH_CURSOR_ID,
        new VectorSearchResultBatch(MOCK_CURSOR_BATCH_SIZE).getBsonResults(),
        "foo.bar");
  }

  /** Returns a MongotCursorBatch that can be used in tests that require one. */
  public static com.xgen.mongot.cursor.serialization.MongotIntermediateCursorBatch
      mockIntermediateMongotCursorBatch() throws Exception {
    return new MongotIntermediateCursorBatch(
        mockMongotCursorBatch(
            MOCK_META_CURSOR_ID,
            new CountMetaBatchProducer(MOCK_CURSOR_BATCH_SIZE)
                .getNextBatch(CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT),
            Optional.empty(),
            "foo.bar",
            Optional.of(MongotCursorResult.Type.META)),
        mockMongotCursorBatch(
            MOCK_SEARCH_CURSOR_ID,
            new SearchResultBatch(MOCK_CURSOR_BATCH_SIZE).getBsonResults(),
            Optional.empty(),
            "foo.bar",
            Optional.of(MongotCursorResult.Type.RESULTS)));
  }
}
