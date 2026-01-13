package com.xgen.mongot.replication.mongodb.common;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_DEFINITION;
import static com.xgen.testing.mongot.mock.index.VectorIndex.MOCK_AUTO_EMBEDDING_INDEX_DEFINITION;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;

import com.google.common.base.Supplier;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.Test;

public class IndexingWorkSchedulerFactoryTest {

  @Test
  public void testContainsSchedulerForEveryIndexingStrategy() {
    IndexingWorkSchedulerFactory indexingWorkSchedulerFactory =
        IndexingWorkSchedulerFactory.create(2, mock(Supplier.class), new SimpleMeterRegistry());

    for (IndexingWorkSchedulerFactory.IndexingStrategy strategy :
        IndexingWorkSchedulerFactory.IndexingStrategy.values()) {
      assertThat(indexingWorkSchedulerFactory.getIndexingWorkSchedulers().containsKey(strategy));
      assertThat(indexingWorkSchedulerFactory.getIndexingWorkSchedulers().get(strategy))
          .isInstanceOf(IndexingWorkScheduler.class);
    }
  }

  @Test
  public void testCreateWithoutEmbeddingStrategy() {
    IndexingWorkSchedulerFactory indexingWorkSchedulerFactory =
        IndexingWorkSchedulerFactory.createWithoutEmbeddingStrategy(2, new SimpleMeterRegistry());

    for (IndexingWorkSchedulerFactory.IndexingStrategy strategy :
        IndexingWorkSchedulerFactory.IndexingStrategy.values()) {
      if (strategy == IndexingWorkSchedulerFactory.IndexingStrategy.EMBEDDING
          || strategy
              == IndexingWorkSchedulerFactory.IndexingStrategy.EMBEDDING_MATERIALIZED_VIEW) {
        assertThat(indexingWorkSchedulerFactory.getIndexingWorkSchedulers())
            .doesNotContainKey(strategy);
      } else {
        assertThat(indexingWorkSchedulerFactory.getIndexingWorkSchedulers()).containsKey(strategy);
        assertThat(indexingWorkSchedulerFactory.getIndexingWorkSchedulers().get(strategy))
            .isInstanceOf(IndexingWorkScheduler.class);
      }
    }
  }

  @Test
  public void testReturnsProperScheduler() {
    IndexingWorkSchedulerFactory indexingWorkSchedulerFactory =
        IndexingWorkSchedulerFactory.create(2, mock(Supplier.class), new SimpleMeterRegistry());

    IndexingWorkScheduler embeddingIndexingWorkScheduler =
        indexingWorkSchedulerFactory.getIndexingWorkScheduler(MOCK_AUTO_EMBEDDING_INDEX_DEFINITION);
    IndexingWorkScheduler defaultIndexingWorkScheduler =
        indexingWorkSchedulerFactory.getIndexingWorkScheduler(MOCK_INDEX_DEFINITION);

    assertThat(embeddingIndexingWorkScheduler).isInstanceOf(EmbeddingIndexingWorkScheduler.class);
    assertThat(defaultIndexingWorkScheduler).isInstanceOf(DefaultIndexingWorkScheduler.class);
  }

  @Test
  public void testThrowsIfIndexingStrategyNotPresent() {
    IndexingWorkSchedulerFactory indexingWorkSchedulerFactory =
        IndexingWorkSchedulerFactory.createWithoutEmbeddingStrategy(2, new SimpleMeterRegistry());

    assertThrows(
        "EMBEDDING indexing strategy is not supported. "
            + "Auto-embedding indexes require the EMBEDDING indexing strategy.",
        IllegalStateException.class,
        () ->
            indexingWorkSchedulerFactory.getIndexingWorkScheduler(
                MOCK_AUTO_EMBEDDING_INDEX_DEFINITION));
  }
}
