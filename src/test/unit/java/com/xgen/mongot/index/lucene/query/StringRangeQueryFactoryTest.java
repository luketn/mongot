package com.xgen.mongot.index.lucene.query;

import static org.mockito.Mockito.mock;

import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.definition.DocumentFieldDefinition;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.QueryOptimizationFlags;
import com.xgen.mongot.index.query.operators.RangeOperator;
import com.xgen.mongot.index.query.points.StringPoint;
import com.xgen.mongot.index.synonym.SynonymRegistry;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.index.analyzer.AnalyzerRegistryBuilder;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.TokenFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.query.operators.OperatorBuilder;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import java.io.IOException;
import java.util.Optional;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.BytesRef;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class StringRangeQueryFactoryTest {

  private static Directory directory;
  private static IndexWriter writer;

  @Before
  public void setUp() throws IOException {
    TemporaryFolder temporaryFolder = TestUtils.getTempFolder();
    directory = new MMapDirectory(temporaryFolder.getRoot().toPath());
    writer = new IndexWriter(directory, new IndexWriterConfig());
    writer.commit();
  }

  @After
  public void tearDown() throws IOException {
    writer.close();
    directory.close();
  }

  @Test
  public void testStringRangeQuery() throws Exception {
    RangeOperator operator =
        OperatorBuilder.range()
            .path("a")
            .stringBounds(
                Optional.of(new StringPoint("a")), Optional.of(new StringPoint("z")), false, false)
            .build();

    DocumentFieldDefinition mappings =
        DocumentFieldDefinitionBuilder.builder()
            .field(
                "a",
                FieldDefinitionBuilder.builder()
                    .token(TokenFieldDefinitionBuilder.builder().build())
                    .build())
            .build();

    IndexOrDocValuesQuery expected =
        new IndexOrDocValuesQuery(
            new TermRangeQuery("$type:token/a", new BytesRef("a"), new BytesRef("z"), false, false),
            SortedSetDocValuesField.newSlowRangeQuery(
                "$type:token/a", new BytesRef("a"), new BytesRef("z"), false, false));

    LuceneSearchTranslation.mapped(mappings).assertTranslatedTo(operator, expected);
  }

  @Test
  public void testStringRangeQueryFailsWhenNotIndexedAsToken() {
    RangeOperator definition =
        OperatorBuilder.range()
            .path("a")
            .stringBounds(
                Optional.of(new StringPoint("a b c")),
                Optional.of(new StringPoint("x y z")),
                false,
                false)
            .build();

    LuceneSearchQueryFactoryDistributor factory = createFactory();
    Assert.assertThrows(
        InvalidQueryException.class,
        () ->
            factory.createQuery(
                definition,
                DirectoryReader.open(directory),
                QueryOptimizationFlags.DEFAULT_OPTIONS));
  }

  private static LuceneSearchQueryFactoryDistributor createFactory() {
    return LuceneSearchQueryFactoryDistributor.create(
        SearchIndexDefinitionBuilder.VALID_INDEX,
        IndexFormatVersion.CURRENT,
        AnalyzerRegistryBuilder.empty(),
        mock(SynonymRegistry.class),
        new IndexMetricsUpdater.QueryingMetricsUpdater(SearchIndex.mockMetricsFactory()),
        true,
        FeatureFlags.getDefault());
  }
}
