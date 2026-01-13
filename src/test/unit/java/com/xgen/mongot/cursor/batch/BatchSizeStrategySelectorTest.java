package com.xgen.mongot.cursor.batch;

import com.xgen.testing.mongot.cursor.batch.BatchCursorOptionsBuilder;
import com.xgen.testing.mongot.index.query.OperatorQueryBuilder;
import com.xgen.testing.mongot.index.query.operators.OperatorBuilder;
import org.junit.Assert;
import org.junit.Test;

public class BatchSizeStrategySelectorTest {

  @Test
  public void testNonStoredSourceQuery() {
    var query =
        OperatorQueryBuilder.builder()
            .operator(OperatorBuilder.exists().path("foo").build())
            .index("test")
            .returnStoredSource(false)
            .build();
    Assert.assertEquals(
        ExponentiallyIncreasingBatchSizeStrategy.class,
        BatchSizeStrategySelector.forQuery(query, BatchCursorOptionsBuilder.empty()).getClass());
  }

  @Test
  public void testStoredSourceQuery() {
    var query =
        OperatorQueryBuilder.builder()
            .operator(OperatorBuilder.exists().path("foo").build())
            .index("test")
            .returnStoredSource(true)
            .build();
    Assert.assertEquals(
        ExponentiallyIncreasingBatchSizeStrategy.class,
        BatchSizeStrategySelector.forQuery(query, BatchCursorOptionsBuilder.empty()).getClass());
  }

  @Test
  public void testDocsRequested() {
    var query =
        OperatorQueryBuilder.builder()
            .operator(OperatorBuilder.exists().path("foo").build())
            .index("test")
            .returnStoredSource(true)
            .build();
    Assert.assertEquals(
        AdjustableBatchSizeStrategy.class,
        BatchSizeStrategySelector.forQuery(
                query, BatchCursorOptionsBuilder.builder().docsRequested(25).build())
            .getClass());
  }

  @Test
  public void testBatchSize() {
    var query =
        OperatorQueryBuilder.builder()
            .operator(OperatorBuilder.exists().path("foo").build())
            .index("test")
            .returnStoredSource(true)
            .build();
    Assert.assertEquals(
        AdjustableBatchSizeStrategy.class,
        BatchSizeStrategySelector.forQuery(
                query, BatchCursorOptionsBuilder.builder().batchSize(25).build())
            .getClass());
  }
}
