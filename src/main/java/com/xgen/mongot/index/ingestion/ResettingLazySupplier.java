package com.xgen.mongot.index.ingestion;

import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import org.bson.BsonReader;
import org.bson.BsonReaderMark;

/**
 * A lazy supplier that calls a "reset" function on close if the value being supplied is requested.
 * Provides a supplier interface given a reader that must be reset to its original position at close
 * time, regardless of whether the reader advances in the course of supplying a value.
 */
class ResettingLazySupplier<T> implements Supplier<T>, AutoCloseable {
  private final BsonReader bsonReader;
  private final Function<BsonReader, T> converter;
  private Optional<BsonReaderMark> beforeRead;
  private Optional<T> value;

  private ResettingLazySupplier(BsonReader bsonReader, Function<BsonReader, T> converter) {
    this.bsonReader = bsonReader;
    this.converter = converter;
    this.beforeRead = Optional.empty();
    this.value = Optional.empty();
  }

  static <T> ResettingLazySupplier<T> create(BsonReader reader, Function<BsonReader, T> converter) {
    return new ResettingLazySupplier<>(reader, converter);
  }

  @Override
  public T get() {
    if (this.value.isPresent()) {
      return this.value.get();
    }

    this.beforeRead = Optional.of(this.bsonReader.getMark());
    T inner = this.converter.apply(this.bsonReader);
    this.value = Optional.of(inner);
    return inner;
  }

  @Override
  public void close() {
    this.beforeRead.ifPresent(BsonReaderMark::reset);
  }
}
