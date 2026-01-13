package com.xgen.mongot.util;

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import org.bson.BsonArray;
import org.bson.BsonValue;

/** A Class for higher level collection manipulations. */
public class CollectionUtils {
  /** Find duplicate elements in a collection, based on E's equals and hashCode. */
  public static <E> Set<E> findDuplicates(Collection<E> collection) {
    Set<E> seen = new HashSet<>();
    Set<E> duplicates = new HashSet<>();

    for (E elem : collection) {
      if (!seen.add(elem)) {
        duplicates.add(elem);
      }
    }
    return duplicates;
  }

  /** Concatenate a few collections into a new List. */
  @SafeVarargs
  public static <E> ImmutableList<E> concat(
      Collection<? extends E> first,
      Collection<? extends E> second,
      Collection<? extends E>... collections) {
    @SuppressWarnings("varargs")
    int totalSize =
        first.size() + second.size() + Arrays.stream(collections).mapToInt(Collection::size).sum();
    var result = ImmutableList.<E>builderWithExpectedSize(totalSize);
    result.addAll(first);
    result.addAll(second);
    for (var c : collections) {
      result.addAll(c);
    }
    return result.build();
  }

  /** Returns a new collection containing elements from `collection` followed by `singleElement`. */
  public static <E> ImmutableList<E> append(Collection<? extends E> collection, E singleElement) {
    var result = ImmutableList.<E>builderWithExpectedSize(collection.size() + 1);
    result.addAll(collection);
    result.add(singleElement);
    return result.build();
  }

  /** Collects a Stream of BsonValues into a {@link BsonArray}. */
  public static Collector<BsonValue, ?, BsonArray> toBsonArray() {
    return Collectors.collectingAndThen(Collectors.toList(), BsonArray::new);
  }

  /**
   * Returns a {@code Collector} that accumulates elements into a {@code Map} whose keys and values
   * are the result of applying the provided mapping functions to the input elements.
   *
   * <p>The mapped keys are expected to be unique. If the mapped keys contain duplicates, this
   * method throws {@link IllegalStateException}.
   *
   * @param <T> the type of the input elements
   * @param <K> the output type of the key mapping function
   * @param <V> the output type of the value mapping function
   * @param keyMapper a mapping function to produce keys
   * @param valueMapper a mapping function to produce values
   * @return a {@code Collector} which collects elements into a {@code Map} whose entries are the
   *     result of applying the key and value mapping function to each input element
   * @see Collectors#toMap(Function, Function)
   */
  @SuppressWarnings("UnsafeCollectors")
  public static <T, K, V> Collector<T, ?, Map<K, V>> toMapUniqueKeys(
      Function<? super T, ? extends K> keyMapper, Function<? super T, ? extends V> valueMapper) {
    return Collectors.toMap(keyMapper, valueMapper);
  }

  /**
   * Returns a stream collector that throws a RuntimeException on duplicate keys.
   *
   * <p>Prefer {@link Collectors#toMap(Function, Function, BinaryOperator)} with an explicit merge
   * function to handle duplicate keys safely.
   *
   * @param keyFunction function to extract keys from stream elements
   * @param valueFunction function to extract values from stream elements
   * @return a collector that creates a map, throwing on duplicate keys
   * @deprecated Use {@link Collectors#toMap(Function, Function, BinaryOperator)} with explicit
   *     merge function instead. This method exists only for migration purposes.
   */
  @Deprecated
  @SuppressWarnings("UnsafeCollectors")
  public static <T, K, V> Collector<T, ?, Map<K, V>> toMapUnsafe(
      Function<? super T, ? extends K> keyFunction,
      Function<? super T, ? extends V> valueFunction) {
    // TODO(CLOUDP-346141): Remove this method once all usages are migrated.
    return Collectors.toMap(keyFunction, valueFunction);
  }

  /**
   * Creates a new map safely reusing keys from input without the need for explicitly handling
   * duplicates because the key is unchanged and hence guaranteed to be unique.
   */
  @SuppressWarnings("UnsafeCollectors")
  public static <K, OLDVALT, NEWVALT> Map<K, NEWVALT> newMapFromKeys(
      Map<K, OLDVALT> input, BiFunction<K, OLDVALT, NEWVALT> oldEntryToNewValueMapper) {
    return input.entrySet().stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                entry -> oldEntryToNewValueMapper.apply(entry.getKey(), entry.getValue())));
  }

  /**
   * Returns a stream collector that throws a RuntimeException on duplicate keys.
   *
   * <p>Prefer {@link Collectors#toUnmodifiableMap(Function, Function, BinaryOperator)} with an
   * explicit merge function to handle duplicate keys safely.
   *
   * @param keyFunction function to extract keys from stream elements
   * @param valueFunction function to extract values from stream elements
   * @return a collector that creates an unmodifiable map, throwing on duplicate keys
   * @deprecated Use {@link Collectors#toUnmodifiableMap(Function, Function, BinaryOperator)} with
   *     explicit merge function instead. This method exists only for migration purposes.
   */
  @Deprecated
  @SuppressWarnings("UnsafeCollectors")
  public static <T, K, V> Collector<T, ?, Map<K, V>> toUnmodifiableMapUnsafe(
      Function<? super T, ? extends K> keyFunction,
      Function<? super T, ? extends V> valueFunction) {
    // TODO(CLOUDP-346141): Remove this method once all usages are migrated.
    return Collectors.toUnmodifiableMap(keyFunction, valueFunction);
  }

  /**
   * Returns a stream collector that throws a RuntimeException on duplicate keys.
   *
   * <p>Prefer {@link Collectors#toConcurrentMap(Function, Function, BinaryOperator)} with an
   * explicit merge function to handle duplicate keys safely.
   *
   * @param keyFunction function to extract keys from stream elements
   * @param valueFunction function to extract values from stream elements
   * @return a collector that creates a concurrent map, throwing on duplicate keys
   * @deprecated Use {@link Collectors#toConcurrentMap(Function, Function, BinaryOperator)} with
   *     explicit merge function instead. This method exists only for migration purposes.
   */
  @Deprecated
  @SuppressWarnings("UnsafeCollectors")
  public static <T, K, V> Collector<T, ?, ConcurrentMap<K, V>> toConcurrentMapUnsafe(
      Function<? super T, ? extends K> keyFunction,
      Function<? super T, ? extends V> valueFunction) {
    // TODO(CLOUDP-346141): Remove this method once all usages are migrated.
    return Collectors.toConcurrentMap(keyFunction, valueFunction);
  }
}
