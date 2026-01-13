package com.xgen.mongot.util.mongodb;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class ConnectionStringBuilderTest {

  @Test
  public void testStandard() throws ConnectionStringUtil.InvalidConnectionStringException {
    var builtConnectionString =
        ConnectionStringBuilder.standard()
            .withAuthenticationCredentials("admin", "password") // kingfisher:ignore
            .withHost("localhost")
            .build()
            .toString();
    var expected = "mongodb://admin:password@localhost/"; // kingfisher:ignore

    assertEquals(expected, builtConnectionString);
  }

  @Test
  public void testWithPort() throws ConnectionStringUtil.InvalidConnectionStringException {
    var builtConnectionString =
        ConnectionStringBuilder.standard()
            .withAuthenticationCredentials("admin", "password") // kingfisher:ignore
            .withHost("localhost:9999")
            .build()
            .toString();
    var expected = "mongodb://admin:password@localhost:9999/"; // kingfisher:ignore

    assertEquals(expected, builtConnectionString);
  }

  @Test
  public void testWithAuthDatabase() throws ConnectionStringUtil.InvalidConnectionStringException {
    var builtConnectionString =
        ConnectionStringBuilder.standard()
            .withAuthenticationCredentials("admin", "password") // kingfisher:ignore
            .withHost("localhost:9999")
            .withAuthenticationDatabase("admin")
            .build()
            .toString();
    var expected = "mongodb://admin:password@localhost:9999/admin"; // kingfisher:ignore

    assertEquals(expected, builtConnectionString);
  }

  @Test
  public void testWithOptions() throws ConnectionStringUtil.InvalidConnectionStringException {
    var builtConnectionString =
        ConnectionStringBuilder.standard()
            .withHost("localhost")
            .withOption("tls", "true")
            .build()
            .toString();
    var expected = "mongodb://localhost/?tls=true";

    assertEquals(expected, builtConnectionString);
  }

  @Test
  public void testFull() throws ConnectionStringUtil.InvalidConnectionStringException {
    var builtConnectionString =
        ConnectionStringBuilder.standard()
            .withAuthenticationCredentials("admin", "password") // kingfisher:ignore
            .withHost("localhost:27017")
            .withAuthenticationDatabase("admin")
            .withOption("tls", "true")
            .withOption("readPreference", "primaryPreferred")
            .build()
            .toString();
    var expected =
        "mongodb://admin:password@localhost:27017/" // kingfisher:ignore
            + "admin?tls=true&readPreference=primaryPreferred";

    assertEquals(expected, builtConnectionString);
  }

  @Test(expected = ConnectionStringUtil.InvalidConnectionStringException.class)
  public void testInvalid() throws ConnectionStringUtil.InvalidConnectionStringException {
    var ignored =
        ConnectionStringBuilder.standard()
            .withHost("localhost")
            .withOption("tls", "true")
            .withOption("ssl", "false")
            .build()
            .toString();
  }
}
