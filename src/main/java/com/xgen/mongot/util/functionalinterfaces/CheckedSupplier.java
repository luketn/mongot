package com.xgen.mongot.util.functionalinterfaces;

@FunctionalInterface
public interface CheckedSupplier<V, E extends Exception> {
  V get() throws E;
}
