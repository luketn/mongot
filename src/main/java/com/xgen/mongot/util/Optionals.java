package com.xgen.mongot.util;

import com.xgen.mongot.util.functionalinterfaces.CheckedFunction;
import com.xgen.mongot.util.functionalinterfaces.CheckedSupplier;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

public class Optionals {

  public static <V> V orElseThrow(Optional<V> opt, String msg) {
    return opt.orElseThrow(() -> new NoSuchElementException(msg));
  }

  public static <V, E extends Exception> V orElseGetChecked(
      Optional<V> opt, CheckedSupplier<? extends V, E> supplier) throws E {
    if (opt.isPresent()) {
      return opt.get();
    } else {
      return supplier.get();
    }
  }

  public static <V> Stream<V> present(Stream<Optional<? extends V>> optionals) {
    return optionals.filter(Optional::isPresent).map(Optional::get);
  }

  /** Applies a mapper which may throw a checked exception, wraps it in runtime exception. */
  public static <U, T, E extends Exception> Optional<U> map(
      Optional<T> lower, CheckedFunction<? super T, ? extends U, E> mapper) {
    return lower.map(
        value -> {
          try {
            return mapper.apply(value);
          } catch (RuntimeException e) {
            throw e;
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
  }

  public static <T extends Comparable<T>> int compareTo(Optional<T> o1, Optional<T> o2) {
    if (o1.isEmpty() && o2.isEmpty()) {
      return 0;
    }

    return o1.map(value -> o2.map(value::compareTo).orElse(-1)).orElse(1);
  }

  @SuppressWarnings("checkstyle:MissingJavadocMethod")
  public static <T extends Comparable<T>> int mapCompareTo(
      Optional<Map<String, T>> maybeFirst, Optional<Map<String, T>> maybeSecond) {
    if (maybeFirst.isEmpty()) {
      return maybeSecond.isPresent() ? -1 : 0;
    }

    if (maybeSecond.isEmpty()) {
      return 1;
    }

    Map<String, T> first = maybeFirst.get();
    Map<String, T> second = maybeSecond.get();

    int sizeComparison = Integer.compare(first.size(), second.size());
    if (sizeComparison != 0) {
      return sizeComparison;
    }

    // Compare entries by key and value
    return first.entrySet().stream()
        .map(
            firstEntry -> {
              Optional<T> secondValue = Optional.ofNullable(second.get(firstEntry.getKey()));
              return secondValue.map(val -> firstEntry.getValue().compareTo(val)).orElse(1);
            })
        .filter(result -> result != 0) // Filter out equal comparisons
        .findFirst()
        .orElse(0); // Return 0 if no differences were found
  }

  /**
   * Applies the mapper which may throw a checked exception. If an exception is thrown it is
   * propagated.
   */
  public static <U, T, E extends Exception> Optional<U> mapOrThrowChecked(
      Optional<T> lower, CheckedFunction<? super T, ? extends U, E> mapper) throws E {
    Objects.requireNonNull(mapper);
    if (lower.isPresent()) {
      T value = lower.get();
      return Optional.ofNullable(mapper.apply(value));
    }

    return Optional.empty();
  }
}
