package com.xgen.mongot.index.lucene.query;

import static org.mockito.Mockito.mock;

import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.analyzer.AnalyzerRegistry;
import com.xgen.mongot.index.query.QueryOptimizationFlags;
import com.xgen.mongot.index.query.operators.RangeOperator;
import com.xgen.mongot.index.query.points.ObjectIdPoint;
import com.xgen.mongot.index.synonym.SynonymRegistry;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.query.operators.OperatorBuilder;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import java.io.IOException;
import java.util.Optional;
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
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ObjectIdRangeQueryFactoryTest {

  private static final String PATH_START = "start";

  private static final ObjectIdPoint OBJECT_ID_FIRST =
      new ObjectIdPoint(new ObjectId("111111111111111111111111"));

  private static final ObjectIdPoint OBJECT_ID_SECOND =
      new ObjectIdPoint(new ObjectId("222222222222222222222222"));

  private static final ObjectId MIN_OBJECT_ID_VALUE = new ObjectId("000000000000000000000000");
  private static final ObjectId MAX_OBJECT_ID_VALUE = new ObjectId("FFFFFFFFFFFFFFFFFFFFFFFF");

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
  public void testObjectIdRangeQueryUpperInclusive() throws Exception {
    boolean lowerInclusive = false;
    boolean upperInclusive = true;
    RangeOperator definition =
        OperatorBuilder.range()
            .path(PATH_START)
            .objectIdBounds(
                Optional.empty(), Optional.of(OBJECT_ID_FIRST), lowerInclusive, upperInclusive)
            .build();

    Query expected =
        createObjectIdRangeQuery(
            "start",
            Optional.empty(),
            Optional.of(OBJECT_ID_FIRST.value()),
            lowerInclusive,
            upperInclusive);

    LuceneSearchQueryFactoryDistributor factory = createFactory();
    Query result =
        factory.createQuery(
            definition, DirectoryReader.open(directory), QueryOptimizationFlags.DEFAULT_OPTIONS);
    Assert.assertEquals("objectId range upper inclusive", expected, result);
  }

  @Test
  public void testObjectIdRangeQueryUpperExclusive() throws Exception {
    boolean lowerInclusive = false;
    boolean upperInclusive = false;
    RangeOperator definition =
        OperatorBuilder.range()
            .path(PATH_START)
            .objectIdBounds(
                Optional.empty(), Optional.of(OBJECT_ID_FIRST), lowerInclusive, upperInclusive)
            .build();

    Query expected =
        createObjectIdRangeQuery(
            "start",
            Optional.empty(),
            Optional.of(OBJECT_ID_FIRST.value()),
            lowerInclusive,
            upperInclusive);

    LuceneSearchQueryFactoryDistributor factory = createFactory();
    Query result =
        factory.createQuery(
            definition, DirectoryReader.open(directory), QueryOptimizationFlags.DEFAULT_OPTIONS);
    Assert.assertEquals("objectId range upper exclusive", expected, result);
  }

  @Test
  public void testObjectIdRangeQueryLowerInclusive() throws Exception {
    boolean lowerInclusive = true;
    boolean upperInclusive = false;
    RangeOperator definition =
        OperatorBuilder.range()
            .path(PATH_START)
            .objectIdBounds(
                Optional.of(OBJECT_ID_FIRST), Optional.empty(), lowerInclusive, upperInclusive)
            .build();

    Query expected =
        createObjectIdRangeQuery(
            "start",
            Optional.of(OBJECT_ID_FIRST.value()),
            Optional.empty(),
            lowerInclusive,
            upperInclusive);

    LuceneSearchQueryFactoryDistributor factory = createFactory();
    Query result =
        factory.createQuery(
            definition, DirectoryReader.open(directory), QueryOptimizationFlags.DEFAULT_OPTIONS);
    Assert.assertEquals("objectId range lower inclusive", expected, result);
  }

  @Test
  public void testObjectIdRangeQueryLowerExclusive() throws Exception {
    boolean lowerInclusive = false;
    boolean upperInclusive = false;
    RangeOperator definition =
        OperatorBuilder.range()
            .path(PATH_START)
            .objectIdBounds(
                Optional.of(OBJECT_ID_FIRST), Optional.empty(), lowerInclusive, upperInclusive)
            .build();

    Query expected =
        createObjectIdRangeQuery(
            "start",
            Optional.of(OBJECT_ID_FIRST.value()),
            Optional.empty(),
            lowerInclusive,
            upperInclusive);

    LuceneSearchQueryFactoryDistributor factory = createFactory();
    Query result =
        factory.createQuery(
            definition, DirectoryReader.open(directory), QueryOptimizationFlags.DEFAULT_OPTIONS);
    Assert.assertEquals("objectId range lower exclusive", expected, result);
  }

  @Test
  public void testObjectIdRangeQueryEmpty() throws Exception {
    RangeOperator definition =
        OperatorBuilder.range()
            .path(PATH_START)
            .objectIdBounds(Optional.empty(), Optional.empty(), false, false)
            .build();
    var factory = createFactory();
    var reader = DirectoryReader.open(directory);

    Assert.assertThrows(
        IllegalStateException.class,
        () -> factory.createQuery(definition, reader, QueryOptimizationFlags.DEFAULT_OPTIONS));
  }

  @Test
  public void testObjectIdRangeQueryLtGte() throws Exception {
    boolean lowerInclusive = true;
    boolean upperInclusive = false;
    RangeOperator definition =
        OperatorBuilder.range()
            .path(PATH_START)
            .objectIdBounds(
                Optional.of(OBJECT_ID_FIRST),
                Optional.of(OBJECT_ID_SECOND),
                lowerInclusive,
                upperInclusive)
            .build();

    Query expected =
        createObjectIdRangeQuery(
            "start",
            Optional.of(OBJECT_ID_FIRST.value()),
            Optional.of(OBJECT_ID_SECOND.value()),
            lowerInclusive,
            upperInclusive);

    LuceneSearchQueryFactoryDistributor factory = createFactory();
    Query result =
        factory.createQuery(
            definition, DirectoryReader.open(directory), QueryOptimizationFlags.DEFAULT_OPTIONS);
    Assert.assertEquals("objectId range lower, upper, and near", expected, result);
  }

  @Test
  public void testObjectIdRangeHighestWindow() throws Exception {
    boolean lowerInclusive = false;
    boolean upperInclusive = false;
    RangeOperator definition =
        OperatorBuilder.range()
            .path(PATH_START)
            .objectIdBounds(
                Optional.of(new ObjectIdPoint(MAX_OBJECT_ID_VALUE)),
                Optional.empty(),
                lowerInclusive,
                upperInclusive)
            .build();

    Query expected =
        createObjectIdRangeQuery(
            "start",
            Optional.of(MAX_OBJECT_ID_VALUE),
            Optional.empty(),
            lowerInclusive,
            upperInclusive);

    LuceneSearchQueryFactoryDistributor factory = createFactory();
    Query result =
        factory.createQuery(
            definition, DirectoryReader.open(directory), QueryOptimizationFlags.DEFAULT_OPTIONS);
    Assert.assertEquals("objectId range lower, upper, and near", expected, result);
  }

  @Test
  public void testObjectIdRangeLowestWindow() throws Exception {
    boolean lowerInclusive = false;
    boolean upperInclusive = false;
    RangeOperator definition =
        OperatorBuilder.range()
            .path(PATH_START)
            .objectIdBounds(
                Optional.empty(),
                Optional.of(new ObjectIdPoint(MIN_OBJECT_ID_VALUE)),
                lowerInclusive,
                upperInclusive)
            .build();

    Query expected =
        createObjectIdRangeQuery(
            "start",
            Optional.empty(),
            Optional.of(MIN_OBJECT_ID_VALUE),
            lowerInclusive,
            upperInclusive);

    LuceneSearchQueryFactoryDistributor factory = createFactory();
    Query result =
        factory.createQuery(
            definition, DirectoryReader.open(directory), QueryOptimizationFlags.DEFAULT_OPTIONS);
    Assert.assertEquals("objectId range lower, upper, and near", expected, result);
  }

  @Test
  public void testObjectIdRangeQueryMultiplePathLtGte() throws Exception {
    boolean lowerInclusive = true;
    boolean upperInclusive = false;
    RangeOperator definition =
        OperatorBuilder.range()
            .path("start")
            .path("end")
            .path("somethingElse")
            .objectIdBounds(
                Optional.of(OBJECT_ID_FIRST),
                Optional.of(OBJECT_ID_SECOND),
                lowerInclusive,
                upperInclusive)
            .build();

    Query expected =
        new BooleanQuery.Builder()
            .add(
                new BooleanClause(
                    createObjectIdRangeQuery(
                        "start",
                        Optional.of(OBJECT_ID_FIRST.value()),
                        Optional.of(OBJECT_ID_SECOND.value()),
                        lowerInclusive,
                        upperInclusive),
                    BooleanClause.Occur.SHOULD))
            .add(
                new BooleanClause(
                    createObjectIdRangeQuery(
                        "end",
                        Optional.of(OBJECT_ID_FIRST.value()),
                        Optional.of(OBJECT_ID_SECOND.value()),
                        lowerInclusive,
                        upperInclusive),
                    BooleanClause.Occur.SHOULD))
            .add(
                new BooleanClause(
                    createObjectIdRangeQuery(
                        "somethingElse",
                        Optional.of(OBJECT_ID_FIRST.value()),
                        Optional.of(OBJECT_ID_SECOND.value()),
                        lowerInclusive,
                        upperInclusive),
                    BooleanClause.Occur.SHOULD))
            .build();

    LuceneSearchQueryFactoryDistributor factory = createFactory();
    Query result =
        factory.createQuery(
            definition, DirectoryReader.open(directory), QueryOptimizationFlags.DEFAULT_OPTIONS);
    Assert.assertEquals("multiple path objectId range lower, upper, and near", expected, result);
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

  private static Query createObjectIdRangeQuery(
      String field,
      Optional<ObjectId> lowerBound,
      Optional<ObjectId> upperBound,
      boolean lowerInclusive,
      boolean upperInclusive) {
    var lower = lowerBound.map(ObjectId::toByteArray).map(BytesRef::new);
    var upper = upperBound.map(ObjectId::toByteArray).map(BytesRef::new);
    return new IndexOrDocValuesQuery(
        new TermRangeQuery(
            "$type:objectId/" + field,
            lower.orElse(null),
            upper.orElse(null),
            lowerInclusive,
            upperInclusive),
        SortedSetDocValuesField.newSlowRangeQuery(
            "$type:objectId/" + field,
            lower.orElse(null),
            upper.orElse(null),
            lowerInclusive,
            upperInclusive));
  }
}
