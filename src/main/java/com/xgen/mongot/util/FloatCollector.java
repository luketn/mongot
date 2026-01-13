package com.xgen.mongot.util;

import java.util.Arrays;
import org.apache.lucene.util.ArrayUtil;

/**
 * This is a dynamically sized list specialized for floats that also accepts and downcasts doubles
 * so that it may be used with {@link java.util.stream.DoubleStream}
 */
public final class FloatCollector {
  private static final float[] EMPTY = new float[0];
  private float[] array;
  private int size = 0;

  public FloatCollector() {
    this.array = EMPTY; // Share immutable reference to optimize for empty streams
  }

  public FloatCollector(int initialCapacity) {
    this.array = new float[initialCapacity];
  }

  private void ensureCapacity(int desiredSize) {
    if (this.array.length < desiredSize) {
      int newSize = Math.max(8, desiredSize);
      this.array = ArrayUtil.grow(this.array, newSize);
    }
  }

  public void add(float f) {
    int slot = this.size++;
    ensureCapacity(this.size);
    this.array[slot] = f;
  }

  public void add(double d) {
    add((float) d);
  }

  public void addAll(FloatCollector other) {
    ensureCapacity(this.size + other.size);
    System.arraycopy(other.array, 0, this.array, this.size, other.size);
    this.size += other.size;
  }

  public int capacity() {
    return this.array.length;
  }

  public float[] toArray() {
    return Arrays.copyOfRange(this.array, 0, this.size);
  }
}
