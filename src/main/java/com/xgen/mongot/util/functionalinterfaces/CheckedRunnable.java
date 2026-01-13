package com.xgen.mongot.util.functionalinterfaces;

@FunctionalInterface
public interface CheckedRunnable<E extends Exception> {
  void run() throws E;
}
