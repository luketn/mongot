package com.xgen.mongot.util;

import com.xgen.mongot.util.functionalinterfaces.CheckedBiExceptionFunction;
import com.xgen.mongot.util.functionalinterfaces.CheckedBinaryOperator;
import com.xgen.mongot.util.functionalinterfaces.CheckedConsumer;
import com.xgen.mongot.util.functionalinterfaces.CheckedFunction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Wraps a {@link Stream} with additional methods for more fluent handling of checked exceptions.
 *
 * <p>As a caveat, note that behind the scenes, these methods force a collection on the stream, so
 * they are unsuitable for use in combination with the parallelization functionality provided by the
 * built-in {@link Stream} API.
 */
public class CheckedStream<T> {
  private final Stream<T> stream;

  private CheckedStream(Stream<T> stream) {
    this.stream = stream;
  }

  /** Creates a <code>CheckedStream</code> from an existing <code>Stream</code>. */
  public static <E> CheckedStream<E> from(Stream<E> stream) {
    return new CheckedStream<>(stream);
  }

  /** Creates a <code>CheckedStream</code> from a collection. */
  public static <E> CheckedStream<E> from(Collection<E> collection) {
    return new CheckedStream<>(collection.stream());
  }

  public static <E> CheckedStream<E> fromSequential(Collection<E> collection) {
    return new CheckedStream<>(collection.stream().sequential());
  }

  /**
   * Applies the provided mapper function to every element of the stream, by iterating over the
   * stream and collecting all elements to a list. Checked exceptions thrown by {@code mapper} are
   * propagated as-is.
   *
   * @param mapper function to apply to stream elements
   */
  public <R, E extends Throwable> List<R> mapAndCollectChecked(
      CheckedFunction<? super T, ? extends R, E> mapper) throws E {
    List<R> results = new ArrayList<>();

    for (Iterator<T> it = this.stream.iterator(); it.hasNext(); ) {
      R result = mapper.apply(it.next());
      results.add(result);
    }

    return results;
  }

  public <R, E1 extends Throwable, E2 extends Throwable>
      List<R> mapAndCollectCheckedBiValueException(
          CheckedBiExceptionFunction<? super T, ? extends R, E1, E2> mapper) throws E1, E2 {
    List<R> results = new ArrayList<>();

    for (Iterator<T> it = this.stream.iterator(); it.hasNext(); ) {
      R result = mapper.apply(it.next());
      results.add(result);
    }
    return results;
  }

  /**
   * Filters the elements of the stream by applying the predicate function to every element of the
   * stream, and collecting resultant elements to a list. Checked exceptions thrown by {@code
   * predicate} are propagated as-is.
   *
   * @param predicate function to apply to stream elements that returns a Boolean.
   */
  public <E extends Throwable> List<T> filterAndCollectChecked(
      CheckedFunction<? super T, Boolean, E> predicate) throws E {
    List<T> filteredResults = new ArrayList<>();

    for (Iterator<T> it = this.stream.iterator(); it.hasNext(); ) {
      T element = it.next();
      if (predicate.apply(element)) {
        filteredResults.add(element);
      }
    }

    return filteredResults;
  }

  /**
   * Applies the provided consumer function to every element of the stream by iterating over the
   * stream. Checked exceptions thrown by {@code consumer} are propagated as-is.
   *
   * @param consumer function to apply to stream elements
   */
  public <E extends Throwable> void forEachChecked(CheckedConsumer<? super T, E> consumer)
      throws E {

    for (Iterator<T> it = this.stream.iterator(); it.hasNext(); ) {
      consumer.accept(it.next());
    }
  }

  /**
   * Collects all elements of the stream to a map whose keys and values are the result of applying
   * the provided mapping functions to the input elements. Checked exceptions thrown by the mapping
   * functions are propagated as-is.
   *
   * <p>Throws {@code IllegalStateException} if the mapped keys contain duplicates, similarly to the
   * standard library collector.
   *
   * <p>The checked exception equivalent to {@code .collect(Collectors.toMap(keyMapper,
   * valueMapper))}.
   */
  public <K, V, KeyMapperExcT extends Throwable, ValueMapperExcT extends Throwable>
      Map<K, V> collectToMapChecked(
          CheckedFunction<? super T, ? extends K, KeyMapperExcT> keyMapper,
          CheckedFunction<? super T, ? extends V, ValueMapperExcT> valueMapper)
          throws KeyMapperExcT, ValueMapperExcT {
    HashMap<K, V> collectedMap = new HashMap<>();
    for (Iterator<T> it = this.stream.iterator(); it.hasNext(); ) {
      T item = it.next();
      if (collectedMap.put(keyMapper.apply(item), valueMapper.apply(item)) != null) {
        throw new IllegalStateException("Duplicate key");
      }
    }
    return collectedMap;
  }

  public <
          K,
          V,
          KeyMapperExcT extends Throwable,
          ValueMapperExcT extends Throwable,
          MergerExcT extends Throwable>
      Map<K, V> collectToMapChecked(
          CheckedFunction<? super T, ? extends K, KeyMapperExcT> keyMapper,
          CheckedFunction<? super T, ? extends V, ValueMapperExcT> valueMapper,
          CheckedBinaryOperator<V, MergerExcT> merger)
          throws KeyMapperExcT, ValueMapperExcT, MergerExcT {
    HashMap<K, V> collectedMap = new HashMap<>();
    for (Iterator<T> it = this.stream.iterator(); it.hasNext(); ) {
      T item = it.next();
      K key = keyMapper.apply(item);
      V value = valueMapper.apply(item);
      V previous = collectedMap.put(key, value);
      // If the key already exists, use the merger function to resolve the conflict.
      if (previous != null) {
        collectedMap.put(key, merger.apply(previous, value));
      }
    }
    return collectedMap;
  }

  public <
          K,
          V,
          KeyMapperExcT extends Throwable,
          ValueMapperExc1T extends Throwable,
          ValueMapperExc2T extends Throwable>
      Map<K, V> collectToMapCheckedBiValueException(
          CheckedFunction<? super T, ? extends K, KeyMapperExcT> keyMapper,
          CheckedBiExceptionFunction<? super T, ? extends V, ValueMapperExc1T, ValueMapperExc2T>
              valueMapper)
          throws KeyMapperExcT, ValueMapperExc1T, ValueMapperExc2T {
    HashMap<K, V> collectedMap = new HashMap<>();
    for (Iterator<T> it = this.stream.iterator(); it.hasNext(); ) {
      T item = it.next();
      if (collectedMap.put(keyMapper.apply(item), valueMapper.apply(item)) != null) {
        throw new IllegalStateException("Duplicate key");
      }
    }
    return collectedMap;
  }

  public <R, E extends Throwable> List<R> flatMapAndCollectChecked(
      CheckedFunction<? super T, ? extends CheckedStream<? extends R>, E> mapper) throws E {
    List<R> results = new ArrayList<>();
    for (Iterator<T> it = this.stream.iterator(); it.hasNext(); ) {
      CheckedStream<? extends R> innerStream = mapper.apply(it.next());
      innerStream.stream.forEach(results::add);
    }
    return results;
  }
}
