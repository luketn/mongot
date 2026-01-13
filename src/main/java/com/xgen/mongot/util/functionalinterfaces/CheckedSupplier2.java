package com.xgen.mongot.util.functionalinterfaces;

@FunctionalInterface
public interface CheckedSupplier2<V, E1 extends Exception, E2 extends Exception> {
  V get() throws E1, E2;
}
