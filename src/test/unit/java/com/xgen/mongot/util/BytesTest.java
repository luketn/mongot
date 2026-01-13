package com.xgen.mongot.util;

import com.xgen.testing.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class BytesTest {

  @Test
  public void testZeroBytes() {
    var zeroBytes = Bytes.ofBytes(0L);
    Assert.assertEquals(0L, zeroBytes.toBytes());
    Assert.assertEquals(0L, zeroBytes.toKibi());
    Assert.assertEquals(0L, zeroBytes.toMebi());
    Assert.assertEquals(0L, zeroBytes.toGibi());
  }

  @Test
  public void testNegativeBytes() {
    Assert.assertThrows(IllegalArgumentException.class, () -> Bytes.ofBytes(-1));
  }

  @Test
  public void testBytesExactFactor() {
    var twoBytes = Bytes.ofBytes(2L);
    Assert.assertEquals(2L, twoBytes.toBytes());
    Assert.assertEquals(0L, twoBytes.toKibi());
    Assert.assertEquals(0L, twoBytes.toMebi());
    Assert.assertEquals(0L, twoBytes.toGibi());

    var twoKibibytes = Bytes.ofKibi(2L);
    Assert.assertEquals(2L * 1024L, twoKibibytes.toBytes());
    Assert.assertEquals(2L, twoKibibytes.toKibi());
    Assert.assertEquals(0L, twoKibibytes.toMebi());
    Assert.assertEquals(0L, twoKibibytes.toGibi());

    var twoMebibytes = Bytes.ofMebi(2L);
    Assert.assertEquals(2L * 1024L * 1024L, twoMebibytes.toBytes());
    Assert.assertEquals(2L * 1024L, twoMebibytes.toKibi());
    Assert.assertEquals(2L, twoMebibytes.toMebi());
    Assert.assertEquals(0L, twoMebibytes.toGibi());

    var twoGibibytes = Bytes.ofGibi(2L);
    Assert.assertEquals(2L * 1024L * 1024L * 1024L, twoGibibytes.toBytes());
    Assert.assertEquals(2L * 1024L * 1024L, twoGibibytes.toKibi());
    Assert.assertEquals(2L * 1024L, twoGibibytes.toMebi());
    Assert.assertEquals(2L, twoGibibytes.toGibi());
  }

  @Test
  public void testBytesNonExactFactorRounds() {
    var twoBytes = Bytes.ofBytes(2L);
    Assert.assertEquals(2L, twoBytes.toBytes());

    var twoPointFiveKibibytes = Bytes.ofBytes((long) 2.5 * 1024L);
    Assert.assertEquals((long) 2.5 * 1024, twoPointFiveKibibytes.toBytes());
    Assert.assertEquals(2L, twoPointFiveKibibytes.toKibi());
    Assert.assertEquals(0L, twoPointFiveKibibytes.toMebi());
    Assert.assertEquals(0L, twoPointFiveKibibytes.toGibi());

    var twoPointFiveMebibytes = Bytes.ofBytes((long) 2.5 * 1024L * 1024L);
    Assert.assertEquals((long) 2.5 * 1024 * 1024, twoPointFiveMebibytes.toBytes());
    Assert.assertEquals((long) 2.5 * 1024, twoPointFiveMebibytes.toKibi());
    Assert.assertEquals(2L, twoPointFiveMebibytes.toMebi());
    Assert.assertEquals(0L, twoPointFiveMebibytes.toGibi());

    var twoPointFiveGibibytes = Bytes.ofBytes((long) 2.5 * 1024L * 1024L * 1024L);
    Assert.assertEquals((long) 2.5 * 1024L * 1024L * 1024L, twoPointFiveGibibytes.toBytes());
    Assert.assertEquals((long) 2.5 * 1024L * 1024L, twoPointFiveGibibytes.toKibi());
    Assert.assertEquals((long) 2.5 * 1024L, twoPointFiveGibibytes.toMebi());
    Assert.assertEquals(2L, twoPointFiveGibibytes.toGibi());
  }

  @Test
  public void testEquals() {
    TestUtils.assertEqualityGroups(
        () -> Bytes.ofBytes(0),
        () -> Bytes.ofBytes(13),
        () -> Bytes.ofKibi(13),
        () -> Bytes.ofMebi(13),
        () -> Bytes.ofGibi(13));
  }

  @Test
  public void testCompareTo() {
    var zeroBytes = Bytes.ofBytes(0);
    Assert.assertEquals(0, zeroBytes.compareTo(Bytes.ofBytes(0)));

    var twoBytes = Bytes.ofBytes(2L);
    Assert.assertEquals(0, twoBytes.compareTo(Bytes.ofBytes(2)));
    Assert.assertTrue(twoBytes.compareTo(zeroBytes) > 0);

    var twoKibibytes = Bytes.ofKibi(2L);
    Assert.assertEquals(0, twoKibibytes.compareTo(Bytes.ofKibi(2L)));
    Assert.assertTrue(twoKibibytes.compareTo(zeroBytes) > 0);
    Assert.assertTrue(twoKibibytes.compareTo(twoBytes) > 0);

    var twoMebibytes = Bytes.ofMebi(2L);
    Assert.assertEquals(0, twoMebibytes.compareTo(Bytes.ofMebi(2L)));
    Assert.assertTrue(twoMebibytes.compareTo(zeroBytes) > 0);
    Assert.assertTrue(twoMebibytes.compareTo(twoBytes) > 0);
    Assert.assertTrue(twoMebibytes.compareTo(twoKibibytes) > 0);

    var twoGibibytes = Bytes.ofGibi(2L);
    Assert.assertEquals(0, twoGibibytes.compareTo(Bytes.ofGibi(2L)));
    Assert.assertTrue(twoGibibytes.compareTo(zeroBytes) > 0);
    Assert.assertTrue(twoGibibytes.compareTo(twoBytes) > 0);
    Assert.assertTrue(twoGibibytes.compareTo(twoKibibytes) > 0);
    Assert.assertTrue(twoGibibytes.compareTo(twoMebibytes) > 0);
  }

  @Test
  public void testAdd() {
    var b1 = Bytes.ofMebi(5);
    var b2 = Bytes.ofKibi(20);
    var sum = b1.add(b2);
    Assert.assertEquals(b1.toBytes() + b2.toBytes(), sum.toBytes());
    Assert.assertNotSame(b1, sum);
    Assert.assertNotSame(b2, sum);
  }

  @Test
  public void testSubstract() {
    var b1 = Bytes.ofMebi(1);
    var b2 = Bytes.ofKibi(64);
    var diff = b1.subtract(b2);
    Assert.assertEquals(b1.toBytes() - b2.toBytes(), diff.toBytes());
    Assert.assertNotSame(b1, diff);
    Assert.assertNotSame(b2, diff);
  }
}
