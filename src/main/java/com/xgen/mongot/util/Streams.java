package com.xgen.mongot.util;

import com.google.common.base.Objects;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class Streams {

  /**
   * enumerate returns a Stream of Enumerations for the supplied list.
   *
   * <p>This is similar to Guava's Streams.mapWithIndex, however mapWithIndex takes in BiFunction
   * that consumes the enumeration, making it incompatible with checked exceptions. The result of
   * enumerate can be passed into mapChecked, or other such utilities that use checked exceptions.
   */
  public static <T> Stream<Enumeration<T>> enumerate(List<T> list) {
    return IntStream.range(0, list.size()).mapToObj(i -> new Enumeration<>(i, list.get(i)));
  }

  /** An Enumeration couples an object of type T with its index in a list. */
  public static class Enumeration<T> {

    private final int index;
    private final T element;

    Enumeration(int index, T element) {
      this.index = index;
      this.element = element;
    }

    public int getIndex() {
      return this.index;
    }

    public T getElement() {
      return this.element;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof Enumeration<?> that)) {
        return false;
      }
      return this.index == that.index && Objects.equal(this.element, that.element);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(this.index, this.element);
    }
  }
}
