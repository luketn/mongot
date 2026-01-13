package com.xgen.mongot.util.functionalinterfaces;

@FunctionalInterface
public interface CheckedConsumer<V, E extends Throwable> {
  void accept(V v) throws E;
}
