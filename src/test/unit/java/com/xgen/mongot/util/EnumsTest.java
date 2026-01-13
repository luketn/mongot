package com.xgen.mongot.util;

import com.google.common.base.CaseFormat;
import org.junit.Assert;
import org.junit.Test;

public class EnumsTest {
  private static final String FIRST_VALUE_NAME = "firstValue";

  private enum SampleEnum {
    FIRST_VALUE,
  }

  @Test
  public void testConvertTo() {
    Assert.assertEquals(
        FIRST_VALUE_NAME, Enums.convertNameTo(CaseFormat.LOWER_CAMEL, SampleEnum.FIRST_VALUE));
  }
}
