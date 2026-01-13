package com.xgen.mongot.index;

/** Is thrown when docs limit is exceeded. */
public class DocsExceededLimitsException extends ExceededLimitsException {

  public DocsExceededLimitsException(String reasons) {
    super(reasons);
  }
}
