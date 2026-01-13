package com.xgen.mongot.util.functionalinterfaces;

/** Interface for a bi function that may throw a checked exception. */
@FunctionalInterface
public interface CheckedBiFunction<A, B, R, E extends Throwable> {
  R apply(A a, B b) throws E;
}
