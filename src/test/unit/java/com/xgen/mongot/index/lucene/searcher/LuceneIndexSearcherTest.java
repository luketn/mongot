package com.xgen.mongot.index.lucene.searcher;

import static com.google.common.truth.Truth.assertThat;

import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.junit.Assert;
import org.junit.Test;

public class LuceneIndexSearcherTest {

  @Test
  public void testSimpleFieldToType() throws IOException {
    try (Directory directory = new ByteBuffersDirectory()) {
      try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
        Document doc = new Document();
        doc.add(
            new NumericDocValuesField(
                FieldName.TypeField.NUMBER_INT64_V2.getLuceneFieldName(
                    FieldPath.newRoot("foo"), Optional.empty()),
                1));
        doc.add(
            new NumericDocValuesField(
                FieldName.TypeField.NUMBER_INT64_V2.getLuceneFieldName(
                    FieldPath.newRoot("bar"), Optional.empty()),
                1));
        writer.addDocument(doc);
        writer.commit();
      }
      IndexReader reader = DirectoryReader.open(directory);
      LuceneIndexSearcher searcher =
          LuceneIndexSearcher.create(
              reader,
              new QueryCacheProvider.DefaultQueryCacheProvider(),
              Optional.empty(),
              Optional.empty(),
              true,
              false,
              Optional.empty());
      assertThat(searcher.getFieldToSortableTypesMapping().rootFieldsToSortableTypes().size())
          .isEqualTo(2);
      assertThat(searcher.getFieldToSortableTypesMapping().rootFieldsToSortableTypes())
          .containsEntry(FieldPath.newRoot("foo"), FieldName.TypeField.NUMBER_INT64_V2);
      assertThat(searcher.getFieldToSortableTypesMapping().rootFieldsToSortableTypes())
          .containsEntry(FieldPath.newRoot("bar"), FieldName.TypeField.NUMBER_INT64_V2);
    }
  }

  @Test
  public void testFieldToTypeMultipleSegments() throws IOException {
    try (Directory directory = new ByteBuffersDirectory()) {
      try (IndexWriter writer =
          new IndexWriter(
              directory, new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
        for (int i = 0; i < 3; i++) {
          Document doc = new Document();
          doc.add(
              new NumericDocValuesField(
                  FieldName.TypeField.NUMBER_INT64_V2.getLuceneFieldName(
                      FieldPath.newRoot("foo"), Optional.empty()),
                  1));
          writer.addDocument(doc);
          writer.flush();
        }
      }
      IndexReader reader = DirectoryReader.open(directory);
      LuceneIndexSearcher searcher =
          LuceneIndexSearcher.create(
              reader,
              new QueryCacheProvider.DefaultQueryCacheProvider(),
              Optional.empty(),
              Optional.empty(),
              true,
              false,
              Optional.empty());
      assertThat(searcher.getFieldToSortableTypesMapping().rootFieldsToSortableTypes().size())
          .isEqualTo(1);
      assertThat(searcher.getFieldToSortableTypesMapping().rootFieldsToSortableTypes())
          .containsEntry(FieldPath.newRoot("foo"), FieldName.TypeField.NUMBER_INT64_V2);
    }
  }

  @Test
  public void testFieldToTypeMultipleSegmentsDiffTypes() throws IOException {
    try (Directory directory = new ByteBuffersDirectory()) {
      try (IndexWriter writer =
          new IndexWriter(
              directory, new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
        Document doc = new Document();
        doc.add(
            new NumericDocValuesField(
                FieldName.TypeField.TOKEN.getLuceneFieldName(
                    FieldPath.newRoot("foo"), Optional.empty()),
                1));
        writer.addDocument(doc);
        writer.flush();

        Document doc2 = new Document();
        doc2.add(
            new NumericDocValuesField(
                FieldName.TypeField.NUMBER_INT64_V2.getLuceneFieldName(
                    FieldPath.newRoot("foo"), Optional.empty()),
                1));
        writer.addDocument(doc2);
        writer.flush();
      }

      IndexReader reader = DirectoryReader.open(directory);
      LuceneIndexSearcher searcher =
          LuceneIndexSearcher.create(
              reader,
              new QueryCacheProvider.DefaultQueryCacheProvider(),
              Optional.empty(),
              Optional.empty(),
              true,
              false,
              Optional.empty());
      assertThat(
              searcher.getFieldToSortableTypesMapping().rootFieldsToSortableTypes().asMap().size())
          .isEqualTo(1);
      assertThat(searcher.getFieldToSortableTypesMapping().rootFieldsToSortableTypes())
          .containsEntry(FieldPath.newRoot("foo"), FieldName.TypeField.NUMBER_INT64_V2);
      assertThat(searcher.getFieldToSortableTypesMapping().rootFieldsToSortableTypes())
          .containsEntry(FieldPath.newRoot("foo"), FieldName.TypeField.TOKEN);
    }
  }

  @Test
  public void testWithFacetsDisabled() throws IOException {
    try (Directory directory = new ByteBuffersDirectory()) {
      try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
        writer.commit();
      }
      IndexReader reader = DirectoryReader.open(directory);
      LuceneIndexSearcher searcher =
          LuceneIndexSearcher.create(
              reader,
              new QueryCacheProvider.DefaultQueryCacheProvider(),
              Optional.empty(),
              Optional.empty(),
              false,
              false,
              Optional.empty());
      Assert.assertTrue(searcher.getFacetsState().isEmpty());
    }
  }

  @Test
  public void testWithFacetsEnabledAndDocumentsWithFacetFieldNotIndexed() throws IOException {
    try (Directory directory = new ByteBuffersDirectory()) {
      try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
        writer.commit();
      }
      IndexReader reader = DirectoryReader.open(directory);
      LuceneIndexSearcher searcher =
          LuceneIndexSearcher.create(
              reader,
              new QueryCacheProvider.DefaultQueryCacheProvider(),
              Optional.empty(),
              Optional.empty(),
              true,
              false,
              Optional.empty());
      Assert.assertTrue(searcher.getFacetsState().isEmpty());
    }
  }

  @Test
  public void testWithFacetsEnabledAndDocumentsWithFacetFieldIndexed() throws IOException {
    try (Directory directory = new ByteBuffersDirectory()) {
      try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new SortedSetDocValuesFacetField("dim", "value"));
        writer.addDocument(new FacetsConfig().build(doc));
        writer.commit();
      }
      IndexReader reader = DirectoryReader.open(directory);
      LuceneIndexSearcher searcher =
          LuceneIndexSearcher.create(
              reader,
              new QueryCacheProvider.DefaultQueryCacheProvider(),
              Optional.empty(),
              Optional.empty(),
              true,
              false,
              Optional.empty());
      Assert.assertFalse(searcher.getFacetsState().isEmpty());
    }
  }

  @Test
  public void testWithFacetsEnabledAndPreviousSearcherHadFacetsState() throws IOException {
    try (Directory directory = new ByteBuffersDirectory()) {
      try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new SortedSetDocValuesFacetField("dim", "val"));
        writer.addDocument(new FacetsConfig().build(doc));
        writer.commit();
      }
      IndexReader reader = DirectoryReader.open(directory);
      LuceneIndexSearcher searcher =
          LuceneIndexSearcher.create(
              reader,
              new QueryCacheProvider.DefaultQueryCacheProvider(),
              Optional.of(
                  LuceneIndexSearcher.create(
                      DirectoryReader.open(directory),
                      new QueryCacheProvider.DefaultQueryCacheProvider(),
                      Optional.empty(),
                      Optional.empty(),
                      true,
                      false,
                      Optional.empty())),
              Optional.empty(),
              true,
              false,
              Optional.empty());
      Assert.assertFalse(searcher.getFacetsState().isEmpty());
    }
  }

  @Test
  public void testWithFacetsEnabledAndExistingPreviousSearcherFacetsStateAndAllDocumentsDeleted()
      throws IOException {
    try (Directory directory = new ByteBuffersDirectory()) {
      try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
        writer.commit(); // emulates a scenario when all documents were deleted
      }
      IndexReader reader = DirectoryReader.open(directory);
      LuceneIndexSearcher searcher =
          LuceneIndexSearcher.create(
              reader,
              new QueryCacheProvider.DefaultQueryCacheProvider(),
              Optional.of(
                  LuceneIndexSearcher.create(
                      DirectoryReader.open(directory),
                      new QueryCacheProvider.DefaultQueryCacheProvider(),
                      Optional.empty(),
                      Optional.empty(),
                      true,
                      false,
                      Optional.empty())),
              Optional.empty(),
              true,
              false,
              Optional.empty());
      // check that state is empty (and no exceptions were thrown)
      Assert.assertTrue(searcher.getFacetsState().isEmpty());
    }
  }

  @Test
  public void testFacetsRefreshTimer() throws Exception {
    try (Directory directory = new ByteBuffersDirectory();
        IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
      Document doc = new Document();
      doc.add(new SortedSetDocValuesFacetField("dim", "val"));
      writer.addDocument(new FacetsConfig().build(doc));
      writer.commit();

      IndexReader reader = DirectoryReader.open(directory);
      IndexMetricsUpdater.QueryingMetricsUpdater queryingMetricsUpdater =
          SearchIndex.mockQueryMetricsUpdater(IndexDefinition.Type.SEARCH);
      var searcher =
          LuceneIndexSearcher.create(
              DirectoryReader.open(directory),
              new QueryCacheProvider.DefaultQueryCacheProvider(),
              Optional.empty(),
              Optional.empty(),
              true,
              true,
              Optional.empty(),
              queryingMetricsUpdater);
      // first build of facets cache does not do any work, so should not trigger metric
      assertThat(
              queryingMetricsUpdater
                  .getTokenFacetsStateRefreshLatencyTimer()
                  .totalTime(TimeUnit.NANOSECONDS))
          .isEqualTo(0);
      var cache = searcher.getTokenFacetsStateCache();
      assertThat(cache).isPresent();

      cache.get().get("dim");
      LuceneIndexSearcher.create(
          reader,
          new QueryCacheProvider.DefaultQueryCacheProvider(),
          Optional.of(
              LuceneIndexSearcher.create(
                  DirectoryReader.open(directory),
                  new QueryCacheProvider.DefaultQueryCacheProvider(),
                  Optional.empty(),
                  Optional.empty(),
                  true,
                  true,
                  Optional.empty(),
                  queryingMetricsUpdater)),
          Optional.empty(),
          true,
          true,
          Optional.empty(),
          queryingMetricsUpdater);
      // searcher refresh with token fields indexed after a query, should trigger metric
      assertThat(
              queryingMetricsUpdater
                  .getTokenFacetsStateRefreshLatencyTimer()
                  .totalTime(TimeUnit.NANOSECONDS))
          .isGreaterThan(0);
    }
  }

  @Test
  public void testEmbeddedSortableTypes() throws IOException {
    try (Directory directory = new ByteBuffersDirectory()) {
      try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
        Document doc = new Document();
        doc.add(
            new NumericDocValuesField(
                FieldName.TypeField.NUMBER_INT64_V2.getLuceneFieldName(
                    FieldPath.newRoot("two"), Optional.of(FieldPath.newRoot("one"))),
                1));
        doc.add(
            new NumericDocValuesField(
                FieldName.TypeField.NUMBER_INT64_V2.getLuceneFieldName(
                    FieldPath.newRoot("three"), Optional.of(FieldPath.newRoot("one"))),
                1));
        doc.add(
            new SortedSetDocValuesField(
                FieldName.TypeField.TOKEN.getLuceneFieldName(
                    FieldPath.parse("one.two"), Optional.empty()),
                new BytesRef("one")));
        doc.add(
            new SortedSetDocValuesField(
                FieldName.TypeField.TOKEN.getLuceneFieldName(
                    FieldPath.parse("one.three"), Optional.empty()),
                new BytesRef("one")));
        writer.addDocument(doc);
        writer.commit();
      }

      IndexReader reader = DirectoryReader.open(directory);
      LuceneIndexSearcher searcher =
          LuceneIndexSearcher.create(
              reader,
              new QueryCacheProvider.DefaultQueryCacheProvider(),
              Optional.empty(),
              Optional.empty(),
              true,
              false,
              Optional.empty());
      assertThat(searcher.getFieldToSortableTypesMapping().rootFieldsToSortableTypes().size())
          .isEqualTo(2);
      assertThat(searcher.getFieldToSortableTypesMapping().embeddedFieldsToSortableTypes().size())
          .isEqualTo(1);

      var rootMapping = searcher.getFieldToSortableTypesMapping().rootFieldsToSortableTypes();
      assertThat(rootMapping).containsEntry(FieldPath.parse("one.two"), FieldName.TypeField.TOKEN);
      assertThat(rootMapping)
          .containsEntry(FieldPath.parse("one.three"), FieldName.TypeField.TOKEN);

      var mapping =
          searcher
              .getFieldToSortableTypesMapping()
              .embeddedFieldsToSortableTypes()
              .get(FieldPath.newRoot("one"));
      assertThat(mapping).isNotNull();
      assertThat(mapping)
          .containsEntry(FieldPath.newRoot("two"), FieldName.TypeField.NUMBER_INT64_V2);
      assertThat(mapping)
          .containsEntry(FieldPath.newRoot("three"), FieldName.TypeField.NUMBER_INT64_V2);
    }
  }
}
