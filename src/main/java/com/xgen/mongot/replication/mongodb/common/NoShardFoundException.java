package com.xgen.mongot.replication.mongodb.common;

import com.xgen.mongot.util.LoggableException;

public class NoShardFoundException extends LoggableException {
  public NoShardFoundException(String reasons) {
    super(reasons);
  }
}
