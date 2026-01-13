package com.xgen.mongot.util.mongodb;

import com.mongodb.MongoException;
import java.util.function.Supplier;

public class CheckedMongoException extends Exception {

  private final MongoException mongoException;

  public CheckedMongoException(MongoException mongoException) {
    super(mongoException);
    this.mongoException = mongoException;
  }

  /**
   * Calls the supplied function, catching any MongoException and wrapping it in a
   * CheckedMongoException.
   */
  public static <V> V checkMongoException(Supplier<V> function) throws CheckedMongoException {
    try {
      return function.get();
    } catch (MongoException e) {
      throw new CheckedMongoException(e);
    }
  }

  public MongoException getMongoException() {
    return this.mongoException;
  }
}
