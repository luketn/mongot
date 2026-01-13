package com.xgen.testing;

import java.lang.reflect.Field;
import org.apache.lucene.util.VectorUtil;
import org.junit.Assert;
import org.junit.Test;

public class PanamaVectorUtilSupportTest {
  @Test
  public void testSimdSupportIsEnabledInLucene() throws Exception {
    Field impl = VectorUtil.class.getDeclaredField("IMPL");
    impl.setAccessible(true);
    Assert.assertEquals(
        "SIMD support (PanamaVectorUtilSupport) is not enabled",
        "org.apache.lucene.internal.vectorization.PanamaVectorUtilSupport",
        impl.get(null).getClass().getName());
  }
}
