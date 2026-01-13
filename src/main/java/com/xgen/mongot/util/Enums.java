package com.xgen.mongot.util;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CaseFormat;

public class Enums {

  @VisibleForTesting
  public static <T extends Enum<T>> String convertNameTo(CaseFormat format, T value) {
    return CaseFormat.UPPER_UNDERSCORE.to(format, value.name());
  }
}
