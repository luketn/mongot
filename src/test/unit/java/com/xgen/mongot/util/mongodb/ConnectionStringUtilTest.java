package com.xgen.mongot.util.mongodb;

import org.junit.Assert;
import org.junit.Test;

public class ConnectionStringUtilTest {

  @Test
  public void testDoesNotThrowForValidConnectionString() throws Exception {
    ConnectionStringUtil.fromString("mongodb://localhost:27017");
  }

  @Test
  public void testThrowsForInvalidConnectionString() {
    Assert.assertThrows(
        ConnectionStringUtil.InvalidConnectionStringException.class,
        () -> ConnectionStringUtil.fromString("this isn't valid"));
  }
}
