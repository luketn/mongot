package com.xgen.mongot.index;

import com.xgen.mongot.util.LoggableException;

/** Signifies some set limit that was exceeded in an index. */
public class ExceededLimitsException extends LoggableException {
  protected ExceededLimitsException(String reasons) {
    super(reasons);
  }
}
