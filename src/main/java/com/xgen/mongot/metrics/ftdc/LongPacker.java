package com.xgen.mongot.metrics.ftdc;

import com.google.errorprone.annotations.Var;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Packs and unpacks signed longs using variable byte encoding. The algorithm is described in
 * https://en.wikipedia.org/wiki/LEB128
 */
class LongPacker {
  // last bit of a byte means the number continues to the next byte
  private static final byte FLAG = (byte) (1 << 7);
  private static final byte SEVEN_BITS_MASK = 0b01111111;
  private static final long MOST_SIGNIFICANT_BITS = ~(long) SEVEN_BITS_MASK;

  private LongPacker() {}

  static void packInto(@Var long number, ByteArrayOutputStream buffer) {
    // we pack 7 bit at a time starting from the least significant ones (little endian)
    // all but the last byte has the 8th bit set to 1

    // keep appending as long as number has more than we can fit in the last byte.
    while (0 != (number & MOST_SIGNIFICANT_BITS)) {
      // add the 7 least significant bits. Use Flag to signify that more bytes are pending for this
      // number.
      buffer.write((((byte) number & SEVEN_BITS_MASK) | FLAG));

      // >>> shifts a zero into the sign bit instead of using sign extension. If we would use >>
      // negative numbers will remain negative and hang in the loop.
      number = number >>> 7;
    }

    // add the last 7 bits (8th bit is off, marking the end of this number)
    buffer.write(((byte) number & SEVEN_BITS_MASK));
  }

  static List<Long> unpack(ByteBuffer buf) {
    List<Long> numbers = new ArrayList<>();

    while (buf.remaining() > 0) {
      long unpacked = unpackOne(buf);
      numbers.add(unpacked);
    }

    return numbers;
  }

  private static long unpackOne(ByteBuffer buf) {
    @Var long current = 0;
    @Var byte b = buf.get();
    @Var int shift = 0;

    while ((b & FLAG) != 0) {
      // FLAG is marked on b, there are more bytes after this one for the current number.
      current += ((long) (b & SEVEN_BITS_MASK) << shift);
      // each packed byte represents 7 unpacked bits of long, advance 7 bits in the unpacked number.
      shift += 7;
      b = buf.get();
    }
    // The current byte isn't marked with FLAG, meaning this is the last packed byte.
    // Add it to current.
    current += ((long) (b & SEVEN_BITS_MASK) << shift);
    return current;
  }
}
