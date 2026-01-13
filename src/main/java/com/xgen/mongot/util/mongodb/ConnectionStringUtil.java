package com.xgen.mongot.util.mongodb;

import com.mongodb.ConnectionString;

public class ConnectionStringUtil {

  public static class InvalidConnectionStringException extends Exception {
    public InvalidConnectionStringException(Throwable cause) {
      super(cause);
    }
  }

  public static ConnectionString fromString(String string) throws InvalidConnectionStringException {
    try {
      return new ConnectionString(string);
    } catch (Exception e) {
      throw new InvalidConnectionStringException(e);
    }
  }
}
