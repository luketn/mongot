package com.xgen.mongot.index.lucene.explain.profiler;

import com.google.common.truth.Truth;
import com.xgen.mongot.index.lucene.explain.query.QueryExecutionContext;
import com.xgen.mongot.index.lucene.explain.query.QueryVisitorQueryExecutionContext;
import com.xgen.mongot.index.lucene.explain.query.QueryVisitorQueryExecutionContextNode;
import com.xgen.mongot.index.lucene.explain.query.RewrittenQueryNodeException;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.testing.mongot.index.lucene.explain.query.RewrittenQueryExecutionContextNodeBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.junit.Test;

public class QueryProfilerTest {

  @Test
  public void testAggregateThreeContexts() throws RewrittenQueryNodeException {
    List<QueryExecutionContext<QueryVisitorQueryExecutionContextNode>> contexts = new ArrayList<>();
    IntStream.range(0, 3).forEach(unused -> contexts.add(new QueryVisitorQueryExecutionContext()));

    var profiler = new QueryProfiler(contexts, Optional.empty());
    profiler.aggregate();

    Truth.assertThat(profiler.getAllExecutionContexts().size()).isEqualTo(3);
    Truth.assertThat(profiler.getMergedNode()).isEmpty();
  }

  @Test
  public void testAggregateTwoContextsMergedNodePresent() throws RewrittenQueryNodeException {
    List<QueryExecutionContext<QueryVisitorQueryExecutionContextNode>> contexts = new ArrayList<>();
    IntStream.range(0, 2).forEach(unused -> contexts.add(new QueryVisitorQueryExecutionContext()));

    var mergedNode =
        RewrittenQueryExecutionContextNodeBuilder.builder().query(new MatchAllDocsQuery()).build();
    var profiler = new QueryProfiler(contexts, Optional.of(mergedNode));
    profiler.aggregate();

    Truth.assertThat(profiler.getAllExecutionContexts().size()).isEqualTo(2);
    Truth.assertThat(profiler.getMergedNode()).isPresent();
    Truth.assertThat(profiler.getMergedNode().get()).isEqualTo(mergedNode);
  }

  @Test
  public void testAggregateSingleContextMergedNodeNotPresent() throws RewrittenQueryNodeException {
    List<QueryExecutionContext<QueryVisitorQueryExecutionContextNode>> contexts = new ArrayList<>();
    contexts.add(new QueryVisitorQueryExecutionContext());

    var profiler = new QueryProfiler(contexts, Optional.empty());
    profiler.aggregate();

    Truth.assertThat(profiler.getAllExecutionContexts().size()).isEqualTo(1);
    Truth.assertThat(profiler.getMergedNode()).isEmpty();
  }

  @Test
  public void testAggregateTwoContextsMergedNodeNotPresent() throws RewrittenQueryNodeException {
    List<QueryExecutionContext<QueryVisitorQueryExecutionContextNode>> contexts = new ArrayList<>();
    IntStream.range(0, 2).forEach(unused -> contexts.add(new QueryVisitorQueryExecutionContext()));
    contexts.forEach(context -> context.getOrCreateNode(new MatchAllDocsQuery())); // set roots

    var profiler = new QueryProfiler(contexts, Optional.empty());
    profiler.aggregate();

    Truth.assertThat(profiler.getAllExecutionContexts()).isEmpty();
    Truth.assertThat(profiler.getMergedNode()).isPresent();
    Truth.assertThat(profiler.getMergedNode().get())
        .isEqualTo(
            RewrittenQueryExecutionContextNodeBuilder.builder()
                .query(new MatchAllDocsQuery())
                .build());
  }

  @Test
  public void testAggregateSingleContextMergedNodePresent() throws RewrittenQueryNodeException {
    List<QueryExecutionContext<QueryVisitorQueryExecutionContextNode>> contexts = new ArrayList<>();
    contexts.add(new QueryVisitorQueryExecutionContext());
    contexts.forEach(context -> context.getOrCreateNode(new MatchAllDocsQuery())); // set roots

    var mergedNode =
        RewrittenQueryExecutionContextNodeBuilder.builder().query(new MatchAllDocsQuery()).build();
    var profiler = new QueryProfiler(contexts, Optional.of(mergedNode));
    profiler.aggregate();

    Truth.assertThat(profiler.getAllExecutionContexts()).isEmpty();
    Truth.assertThat(profiler.getMergedNode()).isPresent();
    Truth.assertThat(profiler.getMergedNode().get())
        .isEqualTo(
            RewrittenQueryExecutionContextNodeBuilder.builder()
                .query(new MatchAllDocsQuery())
                .build());
  }

  @Test
  public void testExplainOutputsMergedNodeAndExecutionContexts() {
    List<QueryExecutionContext<QueryVisitorQueryExecutionContextNode>> contexts = new ArrayList<>();
    IntStream.range(0, 2).forEach(unused -> contexts.add(new QueryVisitorQueryExecutionContext()));
    contexts.forEach(context -> context.getOrCreateNode(new MatchAllDocsQuery())); // set roots

    var mergedNode =
        RewrittenQueryExecutionContextNodeBuilder.builder().query(new MatchAllDocsQuery()).build();
    var profiler = new QueryProfiler(contexts, Optional.of(mergedNode));

    Truth.assertThat(profiler.explainInformation(Explain.Verbosity.EXECUTION_STATS).size())
        .isEqualTo(3);
  }

  @Test
  public void testExplainOutputsContexts() {
    List<QueryExecutionContext<QueryVisitorQueryExecutionContextNode>> contexts = new ArrayList<>();
    IntStream.range(0, 2).forEach(unused -> contexts.add(new QueryVisitorQueryExecutionContext()));
    contexts.forEach(context -> context.getOrCreateNode(new MatchAllDocsQuery())); // set roots

    var profiler = new QueryProfiler(contexts, Optional.empty());

    Truth.assertThat(profiler.explainInformation(Explain.Verbosity.EXECUTION_STATS).size())
        .isEqualTo(2);
  }
}
