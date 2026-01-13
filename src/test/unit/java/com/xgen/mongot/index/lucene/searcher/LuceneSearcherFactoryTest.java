package com.xgen.mongot.index.lucene.searcher;

import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import java.io.IOException;
import java.util.Optional;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.junit.Assert;
import org.junit.Test;

public class LuceneSearcherFactoryTest {

  @Test
  public void testNewSearcher() throws IOException {
    try (Directory directory = new ByteBuffersDirectory()) {
      try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
        writer.commit();
      }

      var indexDefinition =
          SearchIndexDefinitionBuilder.builder()
              .defaultMetadata()
              .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
              .build();

      var factory =
          new LuceneSearcherFactory(
              indexDefinition,
              false,
              new QueryCacheProvider.DefaultQueryCacheProvider(),
              Optional.empty(),
              SearchIndex.mockQueryMetricsUpdater(IndexDefinition.Type.SEARCH));
      var reader = DirectoryReader.open(directory);
      var searcher = factory.newSearcher(reader, Optional.empty());

      Assert.assertNotNull(searcher);
      Assert.assertEquals(reader, searcher.getIndexReader());
      Assert.assertFalse(searcher.getFacetsState().isPresent());
    }
  }
}
