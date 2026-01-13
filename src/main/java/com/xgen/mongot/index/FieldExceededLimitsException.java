package com.xgen.mongot.index;

/** Is thrown when field limit is exceeded. */
public class FieldExceededLimitsException extends ExceededLimitsException {

  public FieldExceededLimitsException(String reasons) {
    super(reasons);
  }
}
