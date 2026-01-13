package com.xgen.mongot.index.lucene.explain.knn;

import com.google.common.collect.ImmutableMap;
import com.xgen.mongot.index.lucene.explain.information.VectorSearchSegmentStatsSpec;
import com.xgen.mongot.index.lucene.explain.information.VectorSearchTracingSpec;
import com.xgen.mongot.index.lucene.explain.timing.ExplainTimings;
import com.xgen.mongot.index.lucene.explain.timing.QueryExecutionArea;
import com.xgen.mongot.util.timers.TimingData;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.FilteredDocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Bits;
import org.bson.BsonInt32;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class KnnInstrumentationHelperTest {

  private static final TopDocs EMPTY_TOP_DOCS =
      new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[] {});

  private static final String vectorFieldName = "vector";
  private static final String filterFieldName = "filter";
  private static final String matchFilterValue = "match";

  private static final BooleanQuery filterQuery =
      new BooleanQuery.Builder()
          .add(
              new ConstantScoreQuery(new TermQuery(new Term(filterFieldName, matchFilterValue))),
              BooleanClause.Occur.MUST)
          .build();

  private static IndexWriter writer;
  private static IndexReader reader;
  private static IndexSearcher searcher;

  @BeforeClass
  public static void setUpClass() throws IOException {
    Directory directory = new ByteBuffersDirectory();
    writer = new IndexWriter(directory, new IndexWriterConfig());

    Document document0 = new Document();
    document0.add(new KnnFloatVectorField(vectorFieldName, new float[] {1, 2, 3, 4}));
    document0.add(new StringField(filterFieldName, matchFilterValue, Field.Store.NO));
    writer.addDocument(document0);

    Document document1 = new Document();
    document1.add(new KnnFloatVectorField(vectorFieldName, new float[] {5, 6, 7, 8}));
    document1.add(new StringField(filterFieldName, "no match", Field.Store.NO));
    writer.addDocument(document1);

    Document document3 = new Document();
    document1.add(new KnnFloatVectorField(vectorFieldName, new float[] {5, 6, 7, 8}));
    document3.add(new StringField(filterFieldName, "no match", Field.Store.NO));
    writer.addDocument(document3);

    writer.commit();

    reader = DirectoryReader.open(directory);
    searcher = new IndexSearcher(reader);
  }

  @Test
  public void meteredApproximateSearch_runsWithFilterPresent_metricsSetCorrectly()
      throws IOException {
    int limit = 10;
    String fieldName = "field";
    Optional<String> leafId = Optional.of("_0");
    VectorSearchExplainer tracingExplainer = new VectorSearchExplainer();
    KnnInstrumentationHelper instrumentationHelper =
        new KnnInstrumentationHelper(tracingExplainer, fieldName, limit, true);

    LeafReaderContext context = reader.getContext().leaves().getFirst();
    Weight filterWeight = filterQuery.createWeight(searcher, ScoreMode.COMPLETE_NO_SCORES, 1.0f);
    BitSet bitSet = getFilterBitSet(context, filterWeight);

    instrumentationHelper.meteredApproximateSearch(context, bitSet, () -> EMPTY_TOP_DOCS);

    VectorSearchExplainer.SegmentStatistics segmentStats =
        tracingExplainer.getSegmentStats(context.id());
    Assert.assertEquals("segment id has to match", leafId, segmentStats.getSegmentId());
    Assert.assertEquals("segment document count has to match", 3, segmentStats.getDocCount());
    Assert.assertEquals(
        "filter matched document count has to match",
        Optional.of(1),
        segmentStats.getFilterMatchedDocsCount());

    ImmutableMap<ExplainTimings.Type, TimingData> timingData =
        segmentStats.getTimings().extractTimingData();
    Assert.assertTrue(
        "approximate time has to be present",
        QueryExecutionArea.notEmptyAreaForType(
                ExplainTimings.Type.VECTOR_SEARCH_APPROXIMATE, timingData)
            .isPresent());
  }

  @Test
  public void meteredApproximateSearch_runsWithNoFilter_metricsSetCorrectly() throws IOException {
    int limit = 10;
    String fieldName = "field";
    Optional<String> leafId = Optional.of("_0");
    VectorSearchExplainer tracingExplainer = new VectorSearchExplainer();
    KnnInstrumentationHelper instrumentationHelper =
        new KnnInstrumentationHelper(tracingExplainer, fieldName, limit, false);

    LeafReaderContext context = reader.getContext().leaves().getFirst();

    instrumentationHelper.meteredApproximateSearch(context, null, () -> EMPTY_TOP_DOCS);

    VectorSearchExplainer.SegmentStatistics segmentStats =
        tracingExplainer.getSegmentStats(context.id());
    Assert.assertEquals("segment id has to match", leafId, segmentStats.getSegmentId());
    Assert.assertEquals("segment document count has to match", 3, segmentStats.getDocCount());
    Assert.assertEquals(
        "filter matched document count has to be empty",
        Optional.empty(),
        segmentStats.getFilterMatchedDocsCount());

    ImmutableMap<ExplainTimings.Type, TimingData> timingData =
        segmentStats.getTimings().extractTimingData();
    Assert.assertTrue(
        "approximate time has to be present",
        QueryExecutionArea.notEmptyAreaForType(
                ExplainTimings.Type.VECTOR_SEARCH_APPROXIMATE, timingData)
            .isPresent());
  }

  @Test
  public void meteredApproximateSearch_runsWithCustomBitsForFilterMatched_metricsSetCorrectly()
      throws IOException {
    int limit = 10;
    String fieldName = "field";
    Optional<String> leafId = Optional.of("_0");
    VectorSearchExplainer tracingExplainer = new VectorSearchExplainer();
    KnnInstrumentationHelper instrumentationHelper =
        new KnnInstrumentationHelper(tracingExplainer, fieldName, limit, true);

    LeafReaderContext context = reader.getContext().leaves().getFirst();

    Bits notBitSet =
        new Bits() {
          @Override
          public boolean get(int index) {
            return index != 0;
          }

          @Override
          public int length() {
            return 3;
          }
        };
    instrumentationHelper.meteredApproximateSearch(context, notBitSet, () -> EMPTY_TOP_DOCS);

    VectorSearchExplainer.SegmentStatistics segmentStats =
        tracingExplainer.getSegmentStats(context.id());
    Assert.assertEquals("segment id has to match", leafId, segmentStats.getSegmentId());
    Assert.assertEquals("segment document count has to match", 3, segmentStats.getDocCount());
    Assert.assertEquals(
        "filter matched document count has to match",
        Optional.of(2),
        segmentStats.getFilterMatchedDocsCount());

    ImmutableMap<ExplainTimings.Type, TimingData> timingData =
        segmentStats.getTimings().extractTimingData();
    Assert.assertTrue(
        "approximate time has to be present",
        QueryExecutionArea.notEmptyAreaForType(
                ExplainTimings.Type.VECTOR_SEARCH_APPROXIMATE, timingData)
            .isPresent());
  }

  @Test
  public void meteredExactSearch_runsWithFilterPresent_metricsSetCorrectly() throws IOException {
    int limit = 10;
    String fieldName = "field";
    Optional<String> leafId = Optional.of("_0");
    VectorSearchExplainer tracingExplainer = new VectorSearchExplainer();
    KnnInstrumentationHelper instrumentationHelper =
        new KnnInstrumentationHelper(tracingExplainer, fieldName, limit, true);

    LeafReaderContext context = reader.getContext().leaves().getFirst();
    Weight filterWeight = filterQuery.createWeight(searcher, ScoreMode.COMPLETE_NO_SCORES, 1.0f);
    FilteredDocIdSetIterator filteredIterator = getDocIdSetIterator(context, filterWeight);

    instrumentationHelper.meteredExactSearch(context, filteredIterator, () -> EMPTY_TOP_DOCS);

    VectorSearchExplainer.SegmentStatistics segmentStats =
        tracingExplainer.getSegmentStats(context.id());
    Assert.assertEquals("segment id has to match", leafId, segmentStats.getSegmentId());
    Assert.assertEquals("segment document count has to match", 3, segmentStats.getDocCount());
    Assert.assertEquals(
        "filter matched document count has to match",
        Optional.of(1),
        segmentStats.getFilterMatchedDocsCount());

    ImmutableMap<ExplainTimings.Type, TimingData> timingData =
        segmentStats.getTimings().extractTimingData();

    Assert.assertTrue(
        "exact time has to be present",
        QueryExecutionArea.notEmptyAreaForType(ExplainTimings.Type.VECTOR_SEARCH_EXACT, timingData)
            .isPresent());
  }

  @Test
  public void examineResultsAfterRescoring_runsWithOneDocDroppedAtRescoring_metricsSetCorrectly() {
    int limit = 3;
    String fieldName = "field";
    VectorSearchExplainer tracingExplainer =
        new VectorSearchExplainer(
            List.of(
                new VectorSearchExplainer.TracingTarget(new BsonInt32(1), 1, false),
                new VectorSearchExplainer.TracingTarget(new BsonInt32(2), 2, false),
                new VectorSearchExplainer.TracingTarget(new BsonInt32(3), 3, false)));
    KnnInstrumentationHelper instrumentationHelper =
        new KnnInstrumentationHelper(tracingExplainer, fieldName, limit, false);

    // mark all of them as visited
    for (VectorSearchExplainer.TracingInformation tracingInformation :
        tracingExplainer.getTracingInformationWithVectors()) {
      tracingInformation.setVisited(true);
      tracingInformation.setExecutionType(
          VectorSearchSegmentStatsSpec.SegmentExecutionType.APPROXIMATE);
    }
    TopDocs topDocs =
        new TopDocs(
            new TotalHits(2, TotalHits.Relation.EQUAL_TO),
            new ScoreDoc[] {
              new ScoreDoc(1, 1), new ScoreDoc(2, 1),
            });
    instrumentationHelper.examineResultsAfterRescoring(topDocs);

    Assert.assertTrue(
        "first document should not be dropped",
        tracingExplainer.getTracingInformation(1).get().getDropReason().isEmpty());
    Assert.assertTrue(
        "second document should not be dropped",
        tracingExplainer.getTracingInformation(2).get().getDropReason().isEmpty());

    Assert.assertEquals(
        "third document has to be dropped with 'Rescoring' reason",
        VectorSearchTracingSpec.DropReason.RESCORING,
        tracingExplainer.getTracingInformation(3).get().getDropReason().get());
  }

  private static BitSet getFilterBitSet(LeafReaderContext context, Weight filterWeight)
      throws IOException {
    return BitSet.of(getDocIdSetIterator(context, filterWeight), reader.maxDoc());
  }

  private static FilteredDocIdSetIterator getDocIdSetIterator(
      LeafReaderContext context, Weight filterWeight) throws IOException {
    Bits liveDocs = context.reader().getLiveDocs();
    return new FilteredDocIdSetIterator(filterWeight.scorer(context).iterator()) {
      @Override
      protected boolean match(int doc) {
        return liveDocs == null || liveDocs.get(doc);
      }
    };
  }
}
