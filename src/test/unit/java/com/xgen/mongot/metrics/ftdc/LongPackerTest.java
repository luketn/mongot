package com.xgen.mongot.metrics.ftdc;

import com.google.common.primitives.Bytes;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.junit.Assert;
import org.junit.Test;

public class LongPackerTest {

  @Test
  public void testPacksToBinary() {
    var buffer = new ByteArrayOutputStream();
    LongPacker.packInto(1L, buffer);
    Assert.assertArrayEquals(new byte[] {1}, buffer.toByteArray());

    LongPacker.packInto(2L, buffer);
    Assert.assertArrayEquals(new byte[] {1, 2}, buffer.toByteArray());

    LongPacker.packInto(128, buffer);
    // 128 needs 8 bits so it is packed to two bytes: 0b10000000, 0b1
    Assert.assertArrayEquals(new byte[] {1, 2, -128, 1}, buffer.toByteArray());
  }

  @Test
  public void testModifyingResultBufferDoesNotCorruptItsState() {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    LongPacker.packInto(1L, buffer);
    var result = buffer.toByteArray();
    // should not modify the actual buffer
    result[0] = 12;
    Assert.assertArrayEquals(new byte[] {1}, buffer.toByteArray());
  }

  @Test
  public void testPackingAndUnpackingRoundTrips() {
    assertRoundTrip(0);
    assertRoundTrip(1);
    assertRoundTrip(1 << 2);
    assertRoundTrip(1 << 8);
    assertRoundTrip(1 << 9);
    assertRoundTrip(0, 1, 1 << 2, 1 << 8, 1 << 9);

    assertRoundTrip(1L << 60);
    assertRoundTrip(1L << 61);
    assertRoundTrip(1L << 62);
    assertRoundTrip(1L << 63);
    assertRoundTrip((1L << 63) + 5);
    assertRoundTrip(Long.MAX_VALUE);
    assertRoundTrip(Long.MAX_VALUE);
    assertRoundTrip(Long.MIN_VALUE, Long.MAX_VALUE);

    assertRoundTrip(-1);
    assertRoundTrip(-0);
    assertRoundTrip(-(1L << 2));
    assertRoundTrip(-(1L << 7));
    assertRoundTrip(-(1L << 8));
    assertRoundTrip(-(1L << 9));
    assertRoundTrip(-(1L << 61));
    assertRoundTrip(-(1L << 62));
    assertRoundTrip(-(1L << 63));
    assertRoundTrip(-(1L << 63) - 5);

    // large and negative numbers on same sequence:
    assertRoundTrip(1, 0, 1L << 60, -1, 42, -42, 1L << 63);
  }

  @Test
  public void testPacksBinaryRepresentation() {
    // test the binary representation directly.

    // 7 ones can be packed with one byte
    assertPacks(List.of(0b1111111), 0b1111111);
    assertPacks(List.of(0b1101110), 0b1101110);

    // 8 bits need two bytes to be packed.
    assertPacks(List.of(0b11111111, 0b1), 0xFF);

    // 17bits need 3 bytes  (7bits + 7bits + 4bits)
    assertPacks(List.of(0b11111111, 0b11111111, 0b111), 0b11111111111111111);
    assertPacks(List.of(0b11111101, 0b11111111, 0b111), 0b11111111111111101);
  }

  /** Asserts that numbers are packed and unpacked to themselves. */
  private void assertRoundTrip(long... numbers) {
    var expected = LongStream.of(numbers).boxed().collect(Collectors.toList());

    var packed = packLongs(numbers);
    var unpacked = LongPacker.unpack(ByteBuffer.wrap(packed));

    Assert.assertEquals(expected, unpacked);
  }

  private void assertPacks(List<Integer> bytes, long... numbers) {
    // we take bytes as a list of integers just because it is more comfortable, there is no byte
    // literal.
    var largerThanAllowed = bytes.stream().anyMatch(x -> x >= 256);
    Assert.assertFalse(largerThanAllowed);
    var expected = Bytes.toArray(bytes);

    var actual = packLongs(numbers);
    Assert.assertArrayEquals(expected, actual);
  }

  private byte[] packLongs(long... numbers) {
    var buffer = new ByteArrayOutputStream();
    for (long number : numbers) {
      LongPacker.packInto(number, buffer);
    }
    return buffer.toByteArray();
  }
}
