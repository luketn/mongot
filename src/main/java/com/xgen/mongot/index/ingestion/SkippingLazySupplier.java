package com.xgen.mongot.index.ingestion;

import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import org.bson.BsonReader;

/**
 * A lazy supplier that calls a "skip" function on close if the value being supplied is not
 * requested. Provides a supplier interface over a value that must be either read or skipped.
 */
class SkippingLazySupplier<T> implements Supplier<T>, AutoCloseable {
  private final BsonReader bsonReader;
  private final Function<BsonReader, T> converter;
  private Optional<T> value;

  private SkippingLazySupplier(BsonReader bsonReader, Function<BsonReader, T> converter) {
    this.bsonReader = bsonReader;
    this.converter = converter;
    this.value = Optional.empty();
  }

  static <T> SkippingLazySupplier<T> create(BsonReader reader, Function<BsonReader, T> converter) {
    return new SkippingLazySupplier<>(reader, converter);
  }

  @Override
  public T get() {
    if (this.value.isPresent()) {
      return this.value.get();
    }

    T inner = this.converter.apply(this.bsonReader);
    this.value = Optional.of(inner);
    return inner;
  }

  @Override
  public void close() {
    if (this.value.isEmpty()) {
      this.bsonReader.skipValue();
    }
  }
}
