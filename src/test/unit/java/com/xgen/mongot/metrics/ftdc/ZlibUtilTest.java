package com.xgen.mongot.metrics.ftdc;

import java.io.IOException;
import java.util.zip.DataFormatException;
import org.junit.Assert;
import org.junit.Test;

public class ZlibUtilTest {
  @Test
  public void testRoundTrip() throws Exception {
    assertRoundTrip("");
    assertRoundTrip("\0");

    assertRoundTrip("x");
    assertRoundTrip("foo");
    assertRoundTrip("\0\n\rAêñüC");

    assertRoundTrip("\0".repeat(1025));
    assertRoundTrip("foo".repeat(100));
    assertRoundTrip("\0\n\rAêñüC".repeat(100));
  }

  private void assertRoundTrip(String data) throws IOException, DataFormatException {
    var compressed = ZlibUtil.zlibCompress(data.getBytes());
    byte[] decompressed = ZlibUtil.zlibDecompress(compressed);
    Assert.assertArrayEquals(data.getBytes(), decompressed);
  }
}
