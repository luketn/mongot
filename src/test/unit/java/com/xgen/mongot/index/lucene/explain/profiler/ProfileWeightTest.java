package com.xgen.mongot.index.lucene.explain.profiler;

import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.xgen.mongot.index.lucene.explain.timing.ExplainTimings;
import java.io.IOException;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class ProfileWeightTest {

  private static IndexWriter writer;
  private static IndexReader reader;
  private static IndexSearcher searcher;

  @BeforeClass
  public static void setUpClass() throws IOException {
    Directory directory = new ByteBuffersDirectory();
    ProfileWeightTest.writer = new IndexWriter(directory, new IndexWriterConfig());

    Document document = new Document();
    document.add(new StringField("foo", "a", Field.Store.NO));
    writer.addDocument(document);
    writer.commit();

    reader = DirectoryReader.open(directory);
    searcher = new IndexSearcher(reader);
  }

  @AfterClass
  public static void tearDownClass() throws IOException {
    reader.close();
    writer.close();
  }

  private ProfileWeight setupWeight(ExplainTimings timings) throws Exception {
    Query query = new TermQuery(new Term("foo", "a"));
    Weight wrappedWeight = searcher.createWeight(query, ScoreMode.COMPLETE, 1.0f);

    return new ProfileWeight(query, wrappedWeight, timings);
  }

  @Test
  public void testMeasuresScorerSupplierFromWrapped() throws Exception {
    ExplainTimings timings = spy(ExplainTimings.builder().build());
    ProfileWeight weight = setupWeight(timings);

    weight.scorerSupplier(reader.getContext().leaves().get(0));
    verify(timings).split(ExplainTimings.Type.CREATE_SCORER);
    verifyNoMoreInteractions(timings);
  }

  @Test
  public void testScorerSupplierMeasuresGet() throws Exception {
    ExplainTimings timings = spy(ExplainTimings.builder().build());
    ProfileWeight weight = setupWeight(timings);

    ScorerSupplier supplier = weight.scorerSupplier(reader.getContext().leaves().get(0));
    reset(timings);

    supplier.get(1L);
    verify(timings).split(ExplainTimings.Type.CREATE_SCORER);
    verifyNoMoreInteractions(timings);
  }

  @Test
  public void testScorerSupplierMeasuresCost() throws Exception {
    ExplainTimings timings = spy(ExplainTimings.builder().build());
    ProfileWeight weight = setupWeight(timings);

    ScorerSupplier supplier = weight.scorerSupplier(reader.getContext().leaves().get(0));
    reset(timings);

    supplier.cost();
    verify(timings).split(ExplainTimings.Type.CREATE_SCORER);
    verifyNoMoreInteractions(timings);
  }
}
