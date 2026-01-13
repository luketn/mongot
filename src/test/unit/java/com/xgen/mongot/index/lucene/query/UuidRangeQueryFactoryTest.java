package com.xgen.mongot.index.lucene.query;

import static org.mockito.Mockito.mock;

import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.analyzer.AnalyzerRegistry;
import com.xgen.mongot.index.query.QueryOptimizationFlags;
import com.xgen.mongot.index.query.operators.RangeOperator;
import com.xgen.mongot.index.query.points.UuidPoint;
import com.xgen.mongot.index.synonym.SynonymRegistry;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.query.operators.OperatorBuilder;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import java.io.IOException;
import java.util.Optional;
import java.util.UUID;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.BytesRef;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class UuidRangeQueryFactoryTest {

  private static final String PATH_START = "start";

  private static final UuidPoint UUID_FIRST =
      new UuidPoint(UUID.fromString("00000000-0000-0000-0000-000000000001"));

  private static final UuidPoint UUID_SECOND =
      new UuidPoint(UUID.fromString("00000000-0000-0000-0000-000000000002"));

  private static final UUID MIN_UUID_VALUE =
      UUID.fromString("00000000-0000-0000-0000-000000000000");
  private static final UUID MAX_UUID_VALUE =
      UUID.fromString("ffffffff-ffff-ffff-ffff-ffffffffffff");

  private static Directory directory;
  private static IndexWriter writer;

  /** set up an index. */
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
  public void testUuidRangeQueryUpperInclusive() throws Exception {
    boolean lowerInclusive = false;
    boolean upperInclusive = true;
    RangeOperator definition =
        OperatorBuilder.range()
            .path(PATH_START)
            .uuidBounds(Optional.empty(), Optional.of(UUID_FIRST), lowerInclusive, upperInclusive)
            .build();

    Query expected =
        createUuidRangeQuery(
            "start",
            Optional.empty(),
            Optional.of(UUID_FIRST.value()),
            lowerInclusive,
            upperInclusive);

    LuceneSearchQueryFactoryDistributor factory = createFactory();
    Query result =
        factory.createQuery(
            definition, DirectoryReader.open(directory), QueryOptimizationFlags.DEFAULT_OPTIONS);
    Assert.assertEquals("uuid range upper inclusive", expected, result);
  }

  @Test
  public void testUuidRangeQueryUpperExclusive() throws Exception {
    boolean lowerInclusive = false;
    boolean upperInclusive = false;
    RangeOperator definition =
        OperatorBuilder.range()
            .path(PATH_START)
            .uuidBounds(Optional.empty(), Optional.of(UUID_FIRST), lowerInclusive, upperInclusive)
            .build();

    Query expected =
        createUuidRangeQuery(
            "start",
            Optional.empty(),
            Optional.of(UUID_FIRST.value()),
            lowerInclusive,
            upperInclusive);

    LuceneSearchQueryFactoryDistributor factory = createFactory();
    Query result =
        factory.createQuery(
            definition, DirectoryReader.open(directory), QueryOptimizationFlags.DEFAULT_OPTIONS);
    Assert.assertEquals("uuid range upper exclusive", expected, result);
  }

  @Test
  public void testUuidRangeQueryLowerInclusive() throws Exception {
    boolean lowerInclusive = true;
    boolean upperInclusive = false;
    RangeOperator definition =
        OperatorBuilder.range()
            .path(PATH_START)
            .uuidBounds(Optional.of(UUID_FIRST), Optional.empty(), lowerInclusive, upperInclusive)
            .build();

    Query expected =
        createUuidRangeQuery(
            "start",
            Optional.of(UUID_FIRST.value()),
            Optional.empty(),
            lowerInclusive,
            upperInclusive);

    LuceneSearchQueryFactoryDistributor factory = createFactory();
    Query result =
        factory.createQuery(
            definition, DirectoryReader.open(directory), QueryOptimizationFlags.DEFAULT_OPTIONS);
    Assert.assertEquals("uuid range lower inclusive", expected, result);
  }

  @Test
  public void testUuidRangeQueryLowerExclusive() throws Exception {
    boolean lowerInclusive = false;
    boolean upperInclusive = false;
    RangeOperator definition =
        OperatorBuilder.range()
            .path(PATH_START)
            .uuidBounds(Optional.of(UUID_FIRST), Optional.empty(), lowerInclusive, upperInclusive)
            .build();

    Query expected =
        createUuidRangeQuery(
            "start",
            Optional.of(UUID_FIRST.value()),
            Optional.empty(),
            lowerInclusive,
            upperInclusive);

    LuceneSearchQueryFactoryDistributor factory = createFactory();
    Query result =
        factory.createQuery(
            definition, DirectoryReader.open(directory), QueryOptimizationFlags.DEFAULT_OPTIONS);
    Assert.assertEquals("uuid range lower exclusive", expected, result);
  }

  @Test
  public void testUuidRangeQueryEmpty() throws Exception {
    RangeOperator definition =
        OperatorBuilder.range()
            .path(PATH_START)
            .uuidBounds(Optional.empty(), Optional.empty(), false, false)
            .build();
    var factory = createFactory();
    var reader = DirectoryReader.open(directory);

    Assert.assertThrows(
        IllegalStateException.class,
        () -> factory.createQuery(definition, reader, QueryOptimizationFlags.DEFAULT_OPTIONS));
  }

  @Test
  public void testUuidRangeQueryLtGte() throws Exception {
    boolean lowerInclusive = true;
    boolean upperInclusive = false;
    RangeOperator definition =
        OperatorBuilder.range()
            .path(PATH_START)
            .uuidBounds(
                Optional.of(UUID_FIRST), Optional.of(UUID_SECOND), lowerInclusive, upperInclusive)
            .build();

    Query expected =
        createUuidRangeQuery(
            "start",
            Optional.of(UUID_FIRST.value()),
            Optional.of(UUID_SECOND.value()),
            lowerInclusive,
            upperInclusive);

    LuceneSearchQueryFactoryDistributor factory = createFactory();
    Query result =
        factory.createQuery(
            definition, DirectoryReader.open(directory), QueryOptimizationFlags.DEFAULT_OPTIONS);
    Assert.assertEquals("uuid range lower, upper, and near", expected, result);
  }

  @Test
  public void testUuidRangeHighestWindow() throws Exception {
    boolean lowerInclusive = false;
    boolean upperInclusive = false;
    RangeOperator definition =
        OperatorBuilder.range()
            .path(PATH_START)
            .uuidBounds(
                Optional.of(new UuidPoint(MAX_UUID_VALUE)),
                Optional.empty(),
                lowerInclusive,
                upperInclusive)
            .build();

    Query expected =
        createUuidRangeQuery(
            "start", Optional.of(MAX_UUID_VALUE), Optional.empty(), lowerInclusive, upperInclusive);

    LuceneSearchQueryFactoryDistributor factory = createFactory();
    Query result =
        factory.createQuery(
            definition, DirectoryReader.open(directory), QueryOptimizationFlags.DEFAULT_OPTIONS);
    Assert.assertEquals("uuid range lower, upper, and near", expected, result);
  }

  @Test
  public void testUuidRangeLowestWindow() throws Exception {
    boolean lowerInclusive = false;
    boolean upperInclusive = false;
    RangeOperator definition =
        OperatorBuilder.range()
            .path(PATH_START)
            .uuidBounds(
                Optional.empty(),
                Optional.of(new UuidPoint(MIN_UUID_VALUE)),
                lowerInclusive,
                upperInclusive)
            .build();

    Query expected =
        createUuidRangeQuery(
            "start", Optional.empty(), Optional.of(MIN_UUID_VALUE), lowerInclusive, upperInclusive);

    LuceneSearchQueryFactoryDistributor factory = createFactory();
    Query result =
        factory.createQuery(
            definition, DirectoryReader.open(directory), QueryOptimizationFlags.DEFAULT_OPTIONS);
    Assert.assertEquals("uuid range lower, upper, and near", expected, result);
  }

  @Test
  public void testUuidRangeQueryMultiplePathLtGte() throws Exception {
    boolean lowerInclusive = true;
    boolean upperInclusive = false;
    RangeOperator definition =
        OperatorBuilder.range()
            .path("start")
            .path("end")
            .path("somethingElse")
            .uuidBounds(
                Optional.of(UUID_FIRST), Optional.of(UUID_SECOND), lowerInclusive, upperInclusive)
            .build();

    Query expected =
        new BooleanQuery.Builder()
            .add(
                new BooleanClause(
                    createUuidRangeQuery(
                        "start",
                        Optional.of(UUID_FIRST.value()),
                        Optional.of(UUID_SECOND.value()),
                        lowerInclusive,
                        upperInclusive),
                    BooleanClause.Occur.SHOULD))
            .add(
                new BooleanClause(
                    createUuidRangeQuery(
                        "end",
                        Optional.of(UUID_FIRST.value()),
                        Optional.of(UUID_SECOND.value()),
                        lowerInclusive,
                        upperInclusive),
                    BooleanClause.Occur.SHOULD))
            .add(
                new BooleanClause(
                    createUuidRangeQuery(
                        "somethingElse",
                        Optional.of(UUID_FIRST.value()),
                        Optional.of(UUID_SECOND.value()),
                        lowerInclusive,
                        upperInclusive),
                    BooleanClause.Occur.SHOULD))
            .build();

    LuceneSearchQueryFactoryDistributor factory = createFactory();
    Query result =
        factory.createQuery(
            definition, DirectoryReader.open(directory), QueryOptimizationFlags.DEFAULT_OPTIONS);
    Assert.assertEquals("multiple path uuid range lower, upper, and near", expected, result);
  }

  private static LuceneSearchQueryFactoryDistributor createFactory() {
    return LuceneSearchQueryFactoryDistributor.create(
        SearchIndexDefinitionBuilder.VALID_INDEX,
        IndexFormatVersion.CURRENT,
        mock(AnalyzerRegistry.class),
        mock(SynonymRegistry.class),
        new IndexMetricsUpdater.QueryingMetricsUpdater(SearchIndex.mockMetricsFactory()),
        false,
        FeatureFlags.getDefault());
  }

  private static Query createUuidRangeQuery(
      String field,
      Optional<UUID> lowerBound,
      Optional<UUID> upperBound,
      boolean lowerInclusive,
      boolean upperInclusive) {
    var lower = lowerBound.map(UUID::toString).map(BytesRef::new);
    var upper = upperBound.map(UUID::toString).map(BytesRef::new);
    return new IndexOrDocValuesQuery(
        new TermRangeQuery(
            "$type:uuid/" + field,
            lower.orElse(null),
            upper.orElse(null),
            lowerInclusive,
            upperInclusive),
        SortedSetDocValuesField.newSlowRangeQuery(
            "$type:uuid/" + field,
            lower.orElse(null),
            upper.orElse(null),
            lowerInclusive,
            upperInclusive));
  }
}
