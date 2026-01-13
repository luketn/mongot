package com.xgen.mongot.index.lucene.explain.profiler;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.truth.Truth;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.lucene.explain.information.LuceneQuerySpecification;
import com.xgen.mongot.index.lucene.explain.information.QueryExplainInformation;
import com.xgen.mongot.index.lucene.explain.information.SearchExplainInformationBuilder;
import com.xgen.mongot.index.lucene.explain.information.TermQuerySpec;
import com.xgen.mongot.index.lucene.explain.query.RewrittenQueryNodeException;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.Test;

public class QueryProfilerFeatureExplainerTest {
  @Test
  public void testFailedAggregate() throws RewrittenQueryNodeException {
    QueryProfiler profiler = spy(new QueryProfiler());
    doThrow(RewrittenQueryNodeException.class).when(profiler).aggregate();
    IndexMetricsUpdater.QueryingMetricsUpdater metricsUpdater =
        SearchIndex.mockQueryMetricsUpdater(IndexDefinition.Type.SEARCH);

    QueryProfilerFeatureExplainer profilerExplainer =
        new QueryProfilerFeatureExplainer(profiler, metricsUpdater);
    profilerExplainer.aggregate();

    Truth.assertThat(metricsUpdater.getFailedExplainQueryAggregate().count()).isEqualTo(1);
  }

  @Test
  public void testManyQueryExplainInfosAllPlansExecution() {
    QueryProfiler profiler = spy(new QueryProfiler());
    QueryProfilerFeatureExplainer profilerExplainer =
        new QueryProfilerFeatureExplainer(
            profiler, SearchIndex.mockQueryMetricsUpdater(IndexDefinition.Type.SEARCH));

    when(profiler.explainInformation(any()))
        .thenReturn(
            IntStream.range(0, 20)
                .mapToObj(
                    unused ->
                        new QueryExplainInformation(
                            Optional.of(FieldPath.newRoot("foo")),
                            LuceneQuerySpecification.Type.TERM_QUERY,
                            Optional.empty(),
                            new TermQuerySpec(FieldPath.newRoot("foo"), "bar"),
                            Optional.empty()))
                .toList());

    SearchExplainInformationBuilder builder = spy(new SearchExplainInformationBuilder());
    profilerExplainer.emitExplanation(Explain.Verbosity.ALL_PLANS_EXECUTION, builder);
    verify(builder).queryExplainInfos(argThat(query -> query.size() == 20));
  }

  @Test
  public void testManyQueryExplainInfosExecutionStats() {
    QueryProfiler profiler = spy(new QueryProfiler());
    QueryProfilerFeatureExplainer profilerExplainer =
        new QueryProfilerFeatureExplainer(
            profiler, SearchIndex.mockQueryMetricsUpdater(IndexDefinition.Type.SEARCH));

    List<QueryExplainInformation> firstTen =
        IntStream.range(0, 10)
            .mapToObj(
                unused ->
                    new QueryExplainInformation(
                        Optional.of(FieldPath.newRoot("foo")),
                        LuceneQuerySpecification.Type.TERM_QUERY,
                        Optional.empty(),
                        new TermQuerySpec(FieldPath.newRoot("foo"), "bar"),
                        Optional.empty()))
            .toList();

    List<QueryExplainInformation> secondTen =
        IntStream.range(0, 10)
            .mapToObj(
                unused ->
                    new QueryExplainInformation(
                        Optional.of(FieldPath.newRoot("abc")),
                        LuceneQuerySpecification.Type.TERM_QUERY,
                        Optional.empty(),
                        new TermQuerySpec(FieldPath.newRoot("abc"), "def"),
                        Optional.empty()))
            .toList();

    when(profiler.explainInformation(any()))
        .thenReturn(Stream.concat(firstTen.stream(), secondTen.stream()).toList());

    SearchExplainInformationBuilder builder = spy(new SearchExplainInformationBuilder());
    profilerExplainer.emitExplanation(Explain.Verbosity.EXECUTION_STATS, builder);
    verify(builder).queryExplainInfos(argThat(query -> query.size() == 10));
    verify(builder).queryExplainInfos(argThat(queryInfos -> queryInfos.equals(secondTen)));
  }
}
