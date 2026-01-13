package com.xgen.mongot.replication.mongodb.common;

import java.util.concurrent.Callable;

@FunctionalInterface
public interface WrapIfThrows<E extends Exception> {
  <T> T wrapIfThrows(Callable<T> callable) throws E;
}
