package com.xgen.mongot.util;

import java.util.Optional;
import org.apache.commons.collections4.Equator;
import org.apache.commons.collections4.functors.DefaultEquator;

public class Equality {

  /**
   * Evaluates to true if (1) both a and b are empty, or (2) if a and b are both present and equal.
   */
  public static <V> boolean equals(Optional<V> a, Optional<V> b) {
    return equals(a, b, DefaultEquator.defaultEquator());
  }

  /** Test equality of two objects using a provided Equator. */
  public static <V> boolean equals(Optional<V> a, Optional<V> b, Equator<V> equator) {
    if (a.isEmpty() || b.isEmpty()) {
      return a.isEmpty() && b.isEmpty();
    }

    return equator.equate(a.get(), b.get());
  }

  /** An equator which always returns true. */
  public static <V> Equator<V> alwaysEqualEquator() {
    return new Equator<>() {
      @Override
      public boolean equate(V o1, V o2) {
        return true;
      }

      @Override
      public int hash(V o) {
        return 0;
      }
    };
  }
}
