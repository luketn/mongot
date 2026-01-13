package com.xgen.mongot.util;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import com.xgen.testing.TestUtils;
import java.util.stream.DoubleStream;
import org.junit.Test;

public class FloatCollectorTest {

  @Test
  public void defaultConstructor() {
    FloatCollector coll = new FloatCollector();
    assertEquals(0, coll.capacity());
    assertArrayEquals(new float[0], coll.toArray(), TestUtils.EPSILON);

    coll.add(1f);
    assertEquals(12, coll.capacity());
    coll.add(2d);
    assertEquals(12, coll.capacity());
    assertArrayEquals(new float[] {1f, 2f}, coll.toArray(), TestUtils.EPSILON);

    coll.addAll(coll);
    assertEquals(12, coll.capacity());
    coll.addAll(coll);
    assertEquals(12, coll.capacity());
    assertArrayEquals(
        new float[] {1f, 2f, 1f, 2f, 1f, 2f, 1f, 2f}, coll.toArray(), TestUtils.EPSILON);
  }

  @Test
  public void sizedConstructor() {
    FloatCollector coll = new FloatCollector(4);
    assertEquals(4, coll.capacity());
    assertArrayEquals(new float[0], coll.toArray(), TestUtils.EPSILON);

    coll.add(1f);
    assertEquals(4, coll.capacity());
    coll.add(2d);
    assertEquals(4, coll.capacity());
    assertArrayEquals(new float[] {1f, 2f}, coll.toArray(), TestUtils.EPSILON);

    coll.addAll(coll);
    assertEquals(4, coll.capacity());
    coll.addAll(coll);
    assertEquals(12, coll.capacity());
    assertArrayEquals(
        new float[] {1f, 2f, 1f, 2f, 1f, 2f, 1f, 2f}, coll.toArray(), TestUtils.EPSILON);
  }

  @Test
  public void emptyToArray() {
    assertArrayEquals(new float[0], new FloatCollector().toArray(), TestUtils.EPSILON);
    assertArrayEquals(new float[0], new FloatCollector(100).toArray(), TestUtils.EPSILON);
  }

  @Test
  public void addAll() {
    FloatCollector coll = new FloatCollector();
    coll.add(1f);
    coll.add(2f);
    FloatCollector other = new FloatCollector();
    other.add(3f);
    other.add(4f);

    coll.addAll(other);

    assertArrayEquals(new float[] {3f, 4f}, other.toArray(), TestUtils.EPSILON);
    assertArrayEquals(new float[] {1f, 2f, 3f, 4f}, coll.toArray(), TestUtils.EPSILON);
  }

  @Test
  public void doubleCollector() {
    float[] expected = new float[8192];
    for (int i = 0; i < expected.length; ++i) {
      expected[i] = i;
    }

    float[] emptyCollector =
        DoubleStream.iterate(0, d -> d + 1)
            .limit(expected.length)
            .collect(FloatCollector::new, FloatCollector::add, FloatCollector::addAll)
            .toArray();
    assertArrayEquals(expected, emptyCollector, TestUtils.EPSILON);

    float[] presized =
        DoubleStream.iterate(0, d -> d + 1)
            .limit(expected.length)
            .collect(
                () -> new FloatCollector(expected.length),
                FloatCollector::add,
                FloatCollector::addAll)
            .toArray();
    assertArrayEquals(expected, presized, TestUtils.EPSILON);
  }
}
