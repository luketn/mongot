package com.xgen.mongot.cursor.batch;

import com.xgen.mongot.index.query.Query;

/**
 * Selects batch size strategy based on the query type or parameters. See more details in
 * https://tinyurl.com/25ujf9ta. Queries with "extractable" limits will have limits passed down in
 * QueryCursorOptions, which will use the limit hint to determine the batch size. Full details on
 * extractable limits here: https://tinyurl.com/y5b649a8
 */
public class BatchSizeStrategySelector {

  private BatchSizeStrategySelector() {}

  // TODO(CLOUDP-280897): should only accept SearchQuery given vector query will always return all
  // results in a single batch
  @SuppressWarnings({"removal"})
  public static BatchSizeStrategy forQuery(Query query, BatchCursorOptions queryCursorOptions) {
    boolean parameterIsPresent =
        queryCursorOptions.getBatchSize().isPresent()
            || queryCursorOptions.getDocsRequested().isPresent();
    if (!parameterIsPresent) {
      // We select exponential strategy with and without stored source to improve performance for
      // customers on MongoDB version < 8.1 since related optimization is not available there
      return new ExponentiallyIncreasingBatchSizeStrategy();
    } else {
      return AdjustableBatchSizeStrategy.create(queryCursorOptions, query.returnStoredSource());
    }
  }
}
