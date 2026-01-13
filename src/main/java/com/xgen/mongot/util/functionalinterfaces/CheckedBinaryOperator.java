package com.xgen.mongot.util.functionalinterfaces;

public interface CheckedBinaryOperator<T, E extends Throwable>
    extends CheckedBiFunction<T, T, T, E> {
  @Override
  T apply(T left, T right) throws E;
}
