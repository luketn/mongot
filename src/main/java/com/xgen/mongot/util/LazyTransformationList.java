package com.xgen.mongot.util;

import com.google.common.collect.Lists;
import java.util.AbstractList;
import java.util.List;
import java.util.RandomAccess;

/**
 * Enables lazy transformation of objects in a given {@link List} as they are traversed upon. This
 * eliminates the need to create a new copy of the target list with eager transformation, could be
 * expensive for large lists.
 *
 * <p>The implementation takes in account if the given list is {@link RandomAccess} to keep
 * optimizations as @{@link Lists#reverse}
 *
 * <p>Note: Callers should take in account that element transformation could be expensive * and
 * consider eager transformation if list is fully traversed more than once.
 */
public class LazyTransformationList {

  public static <S, T> List<T> create(List<S> sourceList, Transformer<S, T> transformer) {
    return (sourceList instanceof RandomAccess)
        ? new LazyTransformationList.RandomAccessImmutableList<>(sourceList, transformer)
        : new LazyTransformationList.ImmutableList<>(sourceList, transformer);
  }

  @FunctionalInterface
  public interface Transformer<S, T> {

    T transform(S source);
  }

  private static class ImmutableList<S, T> extends AbstractList<T> {

    private final List<S> sourceList;
    private final Transformer<S, T> transformer;

    public ImmutableList(List<S> source, Transformer<S, T> transformer) {
      this.sourceList = source;
      this.transformer = transformer;
    }

    /**
     * Returns the transformed element requested index.
     *
     * <p>Note: Callers should take in account that element transformation could be expensive and
     * consider eager transformation if list is fully traversed more than once.
     */
    @Override
    public T get(int index) {
      return this.transformer.transform(this.sourceList.get(index));
    }

    @Override
    public int size() {
      return this.sourceList.size();
    }
  }

  private static class RandomAccessImmutableList<S, T> extends ImmutableList<S, T>
      implements RandomAccess {

    public RandomAccessImmutableList(List<S> source, Transformer<S, T> transformer) {
      super(source, transformer);
    }
  }
}
