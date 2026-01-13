package com.xgen.mongot.util.security;

import static org.junit.Assert.assertEquals;

import java.security.SecureRandom;
import javax.crypto.SecretKeyFactory;
import org.junit.Assert;
import org.junit.Test;

public class SecurityTest {
  @Test
  public void testRegistersProvider() throws Exception {
    Security.installFipsSecurityProvider();
    // Should not throw NoSuchProviderException:
    SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1", Security.FIPS_PROVIDER_NAME);
  }

  @Test
  public void testRegistrationIsIdempotent() {
    Security.installFipsSecurityProvider();
    var provider = java.security.Security.getProvider(Security.FIPS_PROVIDER_NAME);

    Security.installFipsSecurityProvider();
    // should be the same provider - we only register once
    Assert.assertSame(provider, java.security.Security.getProvider(Security.FIPS_PROVIDER_NAME));
  }

  @Test
  public void testGetSecureRandom() {
    SecureRandom sr = Security.getSecureRandom();
    assertEquals("BCFIPS_RNG", sr.getProvider().getName());
  }
}
