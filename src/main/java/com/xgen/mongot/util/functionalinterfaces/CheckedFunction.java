package com.xgen.mongot.util.functionalinterfaces;

/** Interface for a function that may throw a checked exception. */
@FunctionalInterface
public interface CheckedFunction<T, R, E extends Throwable> {

  R apply(T t) throws E;
}
