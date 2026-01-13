package com.xgen.mongot.util.functionalinterfaces;

@FunctionalInterface
public interface CheckedBiConsumer<K, V, E extends Throwable> {
  void accept(K k, V v) throws E;
}
