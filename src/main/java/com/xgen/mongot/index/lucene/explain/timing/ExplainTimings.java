package com.xgen.mongot.index.lucene.explain.timing;

import static com.google.common.collect.Maps.toImmutableEnumMap;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Streams;
import com.xgen.mongot.util.CollectionUtils;
import com.xgen.mongot.util.timers.InvocationCountingTimer;
import com.xgen.mongot.util.timers.ThreadSafeInvocationCountingTimer;
import com.xgen.mongot.util.timers.TimingData;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Stream;

/**
 * This class is thread-safe due to the underlying {@code Map<Type, InvocationCountingTimer>} being
 * instantiated only when the ExplainTimings is created using ThreadSafeInvocationCountingTimer's,
 * and the structure of the map is never modified.
 */
public class ExplainTimings {
  public enum Type {
    VECTOR_EXECUTION("vectorExecution"),
    CREATE_WEIGHT("createWeight"),
    CREATE_SCORER("createScorer"),
    NEXT_DOC("nextDoc"),
    ADVANCE("nextDoc"),
    MATCH("refineRoughMatch"),
    SCORE("score"),
    SHALLOW_ADVANCE("shallowAdvance"),
    COMPUTE_MAX_SCORE("computeMaxScore"),
    SET_MIN_COMPETITIVE_SCORE("setMinCompetitiveScore"),
    COLLECT("collect"),
    COMPETITIVE_ITERATOR("competitiveIterator"),
    SET_SCORER("setScorer"),

    // sort specific types
    SET_BOTTOM("setBottom"),
    COMPARE_BOTTOM("compareBottom"),
    COMPARE_TOP("compareTop"),
    SET_HITS_THRESHOLD_REACHED("setHitsThresholdReached"),

    // facet specific types
    GENERATE_FACET_COUNTS("generateFacetCounts"),

    // highlight specific
    EXECUTE_HIGHLIGHT("executeHighlight"),
    SETUP_HIGHLIGHT("setupHighlight"),

    // result materialization specific
    RETRIEVE_AND_SERIALIZE("retrieveAndSerialize"),

    VECTOR_SEARCH_APPROXIMATE("vectorSearchApproximate"),
    VECTOR_SEARCH_EXACT("vectorSearchExact");

    private final String name;

    Type(String name) {
      this.name = name;
    }

    public String getName() {
      return this.name;
    }
  }

  private final ImmutableMap<Type, ThreadSafeInvocationCountingTimer> timers;

  @VisibleForTesting
  public ExplainTimings(Function<Type, ThreadSafeInvocationCountingTimer> timerFactory) {
    this.timers =
        Maps.immutableEnumMap(
            new EnumMap<>(
                Arrays.stream(Type.values())
                    .collect(CollectionUtils.toMapUnsafe(Function.identity(), timerFactory))));
  }

  public static ExplainTimings merge(ExplainTimings first, ExplainTimings second) {
    return new ExplainTimings(
        (type) ->
            ThreadSafeInvocationCountingTimer.merge(
                Ticker.systemTicker(), first.ofType(type), second.ofType(type)));
  }

  public static ImmutableMap<Type, TimingData> combineTimingData(
      Map<Type, TimingData> a, ExplainTimings b) {
    return Streams.concat(a.entrySet().stream(), b.stream()).collect(toExplainTimingData());
  }

  public static Collector<Map.Entry<Type, TimingData>, ?, ImmutableMap<Type, TimingData>>
      toExplainTimingData() {
    return toImmutableEnumMap(Map.Entry::getKey, Map.Entry::getValue, TimingData::sum);
  }

  public ImmutableMap<Type, TimingData> extractTimingData() {
    return stream().collect(ExplainTimings.toExplainTimingData());
  }

  /** Returns <code>true</code> if the <code>TimingData</code> for all entries is empty. */
  public boolean allTimingDataIsEmpty() {
    return stream().map(Map.Entry::getValue).allMatch(TimingData::isEmpty);
  }

  public InvocationCountingTimer ofType(Type type) {
    return this.timers.get(type);
  }

  public InvocationCountingTimer.SafeClosable split(Type type) {
    return this.timers.get(type).split();
  }

  public Stream<Map.Entry<Type, TimingData>> stream() {
    return this.timers.entrySet().stream()
        .map(
            entry ->
                new AbstractMap.SimpleImmutableEntry<>(
                    entry.getKey(), entry.getValue().getTimingData()));
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private Optional<Ticker> ticker = Optional.empty();
    private boolean ignoreInvocationCounts = false;

    private Builder() {}

    public Builder withTicker(Ticker ticker) {
      this.ticker = Optional.of(ticker);
      return this;
    }

    public Builder ignoreInvocationCounts() {
      this.ignoreInvocationCounts = true;
      return this;
    }

    public ExplainTimings build() {
      var ticker = this.ticker.orElse(Ticker.systemTicker());
      if (this.ignoreInvocationCounts) {
        return new ExplainTimings(
            ignored ->
                new ThreadSafeInvocationCountingTimer(
                    ticker,
                    (invocationCount, elapsedNanos) ->
                        new TimingData.NoInvocationCountTimingData(elapsedNanos)));
      }

      return new ExplainTimings(ignored -> new ThreadSafeInvocationCountingTimer(ticker));
    }
  }
}
