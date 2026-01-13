package com.xgen.mongot.index;

import com.xgen.mongot.util.LoggableException;
import com.xgen.mongot.util.UserFacingException;

public class IndexUnavailableException extends LoggableException implements UserFacingException {

  public IndexUnavailableException(String message) {
    super(message);
  }
}
