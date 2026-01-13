package com.xgen.testing.fakes.mongodb;

import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import java.util.Iterator;
import java.util.List;

/**
 * MockMongoCursor is a minimal wrapper around a provided list of objects to implement just the
 * iterator interface of MongoCursor.
 */
public class MockMongoCursor<T> implements com.mongodb.client.MongoCursor<T> {

  private final Iterator<T> iterator;

  public MockMongoCursor(List<T> contents) {
    this.iterator = contents.iterator();
  }

  @Override
  public void close() {}

  @Override
  public boolean hasNext() {
    return this.iterator.hasNext();
  }

  @Override
  public T next() {
    return this.iterator.next();
  }

  @Override
  public int available() {
    throw new UnsupportedOperationException();
  }

  @Override
  public T tryNext() {
    return this.iterator.next();
  }

  @Override
  public ServerCursor getServerCursor() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ServerAddress getServerAddress() {
    throw new UnsupportedOperationException();
  }
}
