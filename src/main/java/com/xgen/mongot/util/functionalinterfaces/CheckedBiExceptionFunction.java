package com.xgen.mongot.util.functionalinterfaces;

/** Interface for a function that may throw more than one checked exception. */
@FunctionalInterface
public interface CheckedBiExceptionFunction<T, R, E1 extends Throwable, E2 extends Throwable> {

  R apply(T t) throws E1, E2;
}
