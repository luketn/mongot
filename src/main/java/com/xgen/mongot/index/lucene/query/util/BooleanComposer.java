package com.xgen.mongot.index.lucene.query.util;

import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.util.CheckedStream;
import com.xgen.mongot.util.functionalinterfaces.CheckedBiFunction;
import com.xgen.mongot.util.functionalinterfaces.CheckedFunction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;

public class BooleanComposer {

  private BooleanComposer() {}

  /**
   * Create a required clause that contributes to scoring. Prefer {@link #filterClause(Query)} if
   * scoring is not required.
   */
  public static BooleanClause mustClause(Query query) {
    return new BooleanClause(query, BooleanClause.Occur.MUST);
  }

  /** Negates a query clause. */
  public static BooleanClause mustNotClause(Query query) {
    return new BooleanClause(query, BooleanClause.Occur.MUST_NOT);
  }

  /** Create an optional clause that contributes to scoring. */
  public static BooleanClause shouldClause(Query query) {
    return new BooleanClause(query, BooleanClause.Occur.SHOULD);
  }

  /** Create a required clause that does not contribute to scoring. */
  public static BooleanClause filterClause(Query query) {
    return new BooleanClause(query, BooleanClause.Occur.FILTER);
  }

  public static Query queryFor(BooleanClause... clauses) {
    return Arrays.stream(clauses).collect(collector(clause -> clause));
  }

  /**
   * Create a disjunction of optional clauses without scoring.
   *
   * <p>A document must match at least one clause in order to match this query. All matches are
   * assigned a score of 1.0 regardless of the number of clauses they match.
   *
   * <p>This function should be preferred for ORing clauses known to be mutually exclusive (e.g. *
   * querying DOUBLE and DOUBLE_MULTIPLE fields) because it is easily optimized by Lucene.
   */
  public static Query constantScoreDisjunction(Query... clauses) {
    return new ConstantScoreQuery(BooleanComposer.should(clauses));
  }

  private static Query composeQuery(List<? extends Query> queries, BooleanClause.Occur occur) {
    if (queries.size() == 1) {
      return queries.get(0);
    }

    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    queries.forEach(q -> builder.add(q, occur));
    return builder.build();
  }

  /**
   * Create a disjunction of optional clauses with scoring.
   *
   * <p>See {@link #constantScoreDisjunction(Query...)} for faster disjunctions that don't require
   * scoring.
   */
  public static Query should(Query... queries) {
    return Arrays.stream(queries).collect(collector(BooleanComposer::shouldClause));
  }

  public static Query must(Query... queries) {
    return Arrays.stream(queries).collect(collector(BooleanComposer::mustClause));
  }

  public static Query filter(Query... queries) {
    return Arrays.stream(queries).collect(collector(BooleanComposer::filterClause));
  }

  public static Query mustNot(Query... queries) {
    return Arrays.stream(queries).collect(collector(BooleanComposer::mustNotClause));
  }

  public static <T> Collector<T> collector(Function<T, BooleanClause> mapper) {
    return new Collector<>(mapper);
  }

  public static Collector<Query> collector(BooleanClause.Occur occur) {
    return switch (occur) {
      case SHOULD -> new Collector<>(BooleanComposer::shouldClause);
      case MUST -> new Collector<>(BooleanComposer::mustClause);
      case FILTER -> new Collector<>(BooleanComposer::filterClause);
      case MUST_NOT -> new Collector<>(BooleanComposer::mustNotClause);
    };
  }

  /**
   * Collects a stream of queries into a {@link BooleanQuery} without scoring.
   *
   * <p>This function should be preferred for ORing clauses known to be mutually exclusive (e.g.
   * querying DOUBLE and DOUBLE_MULTIPLE fields) because it is easily optimized by Lucene.
   */
  public static java.util.stream.Collector<? super Query, ?, ConstantScoreQuery>
      toConstantScoreDisjunction() {
    return Collectors.collectingAndThen(
        BooleanComposer.collector(Occur.SHOULD), ConstantScoreQuery::new);
  }

  /**
   * Calls {@link BooleanQuery.Builder} to collect given objects into Lucene Boolean Query using
   * supplied mapper to convert them to must/mustNot/should/filter clauses.
   */
  public static class Collector<T>
      implements java.util.stream.Collector<T, BooleanQuery.Builder, Query> {

    private final Function<T, BooleanClause> mapper;

    public Collector(Function<T, BooleanClause> mapper) {
      this.mapper = mapper;
    }

    @Override
    public Supplier<BooleanQuery.Builder> supplier() {
      return BooleanQuery.Builder::new;
    }

    @Override
    public BiConsumer<BooleanQuery.Builder, T> accumulator() {
      return (builder, query) -> builder.add(this.mapper.apply(query));
    }

    @Override
    public BinaryOperator<BooleanQuery.Builder> combiner() {
      return (b1, b2) -> {
        b1.build().forEach(b2::add);
        return b2;
      };
    }

    @Override
    public Function<BooleanQuery.Builder, Query> finisher() {
      return BooleanQuery.Builder::build;
    }

    @Override
    public Set<Characteristics> characteristics() {
      return Set.of();
    }
  }

  /**
   * A stream like utility to map elements of type T to Query, and then reduce to a single boolean
   * query with a should, must, mustNot, or filter clause.
   */
  public static class StreamUtils<T> {
    private final List<T> elements;

    private StreamUtils(List<T> elements) {
      this.elements = elements;
    }

    public static <T> StreamUtils<T> from(Stream<T> elements) {
      return new StreamUtils<>(elements.collect(Collectors.toList()));
    }

    public static <T> StreamUtils<T> from(List<T> elements) {
      return new StreamUtils<>(elements);
    }

    public Query map(Function<T, ? extends Query> builder, BooleanClause.Occur occur) {
      List<Query> queries = this.elements.stream().map(builder).collect(Collectors.toList());
      return composeQuery(queries, occur);
    }

    /**
     * Apply boolean operator to a builder taking two arguments, with the second argument bound to
     * one supplied.
     */
    public <S> Query mapWithBoundSecondArgument(
        S secondArgument,
        CheckedBiFunction<T, S, ? extends Query, InvalidQueryException> builder,
        BooleanClause.Occur occur)
        throws InvalidQueryException {

      List<Query> queries = new ArrayList<>();
      for (var element : this.elements) {
        queries.add(builder.apply(element, secondArgument));
      }

      return composeQuery(queries, occur);
    }

    /** Map using checked function. May throw InvalidQueryException */
    public Query mapChecked(
        CheckedFunction<T, ? extends Query, InvalidQueryException> builder,
        BooleanClause.Occur occur)
        throws InvalidQueryException {
      List<Query> queries = CheckedStream.from(this.elements).mapAndCollectChecked(builder);
      return composeQuery(queries, occur);
    }

    /**
     * Apply boolean operator to a builder that takes two arguments and throws a checked exception,
     * binding second argument to one supplied to this function. May throw InvalidQueryException.
     */
    public <S> Query mapCheckedWithBoundSecondArgument(
        S secondArgument,
        CheckedBiFunction<T, S, ? extends Query, InvalidQueryException> builder,
        BooleanClause.Occur occur)
        throws InvalidQueryException {
      List<Query> queries =
          CheckedStream.from(this.elements)
              .mapAndCollectChecked(element -> builder.apply(element, secondArgument));
      return composeQuery(queries, occur);
    }

    /** Ignores any empty optionals returned by the builder. */
    public Query mapOptional(
        Function<T, Optional<? extends Query>> builder, BooleanClause.Occur occur) {
      List<Query> queries =
          this.elements.stream()
              .map(builder)
              .filter(Optional::isPresent)
              .map(Optional::get)
              .collect(Collectors.toList());
      return composeQuery(queries, occur);
    }

    /** Ignores any empty optionals returned by the builder. May throw InvalidQueryException. */
    public Query mapOptionalChecked(
        CheckedFunction<T, Optional<? extends Query>, InvalidQueryException> builder,
        BooleanClause.Occur occur)
        throws InvalidQueryException {
      List<Query> queries =
          CheckedStream.from(this.elements).mapAndCollectChecked(builder).stream()
              .filter(Optional::isPresent)
              .map(Optional::get)
              .collect(Collectors.toList());
      return composeQuery(queries, occur);
    }
  }
}
