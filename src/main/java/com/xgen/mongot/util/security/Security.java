package com.xgen.mongot.util.security;

import static java.security.DrbgParameters.Capability.PR_AND_RESEED;

import com.xgen.mongot.util.Crash;
import java.security.DrbgParameters;
import java.security.SecureRandom;
import org.bouncycastle.crypto.CryptoServicesRegistrar;
import org.bouncycastle.crypto.fips.FipsDRBG;
import org.bouncycastle.crypto.util.BasicEntropySourceProvider;
import org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider;
import org.bouncycastle.jsse.provider.BouncyCastleJsseProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Security {
  public static final String FIPS_PROVIDER_NAME = BouncyCastleFipsProvider.PROVIDER_NAME;
  public static final String JSSE_PROVIDER_NAME = BouncyCastleJsseProvider.PROVIDER_NAME;

  private static final Logger LOG = LoggerFactory.getLogger(Security.class);
  private static final int SECURE_RANDOM_STRENGTH_IN_BITS = 256;

  /** Installs security provider that can later be referenced using FIPS_PROVIDER_NAME. */
  public static synchronized void installFipsSecurityProvider() {
    if (java.security.Security.getProvider(FIPS_PROVIDER_NAME) == null
        || java.security.Security.getProvider(JSSE_PROVIDER_NAME) == null) {
      LOG.info("installing FIPS security providers");

      registerFipsSecureRandom();

      // Install JCA/JCE BCFIPS Provider
      java.security.Security.addProvider(new BouncyCastleFipsProvider());

      // Install JSSE FIPS Provider
      java.security.Security.addProvider(new BouncyCastleJsseProvider("fips:BCFIPS"));
    }
  }

  /** Registers SecureRandom with FIPS-compliant implementation */
  private static void registerFipsSecureRandom() {
    SecureRandom secureRandom =
        Crash.because("failed to get a SecureRandom instance")
            .ifThrows(
                () ->
                    SecureRandom.getInstance(
                        "DRBG",
                        DrbgParameters.instantiation(
                            SECURE_RANDOM_STRENGTH_IN_BITS, PR_AND_RESEED, null)));
    CryptoServicesRegistrar.setSecureRandom(
        FipsDRBG.SHA512_HMAC
            .fromEntropySource(new BasicEntropySourceProvider(secureRandom, true))
            .build(null, true));
  }

  /** Gets the registered SecureRandom from {@link #registerFipsSecureRandom} */
  public static SecureRandom getSecureRandom() {
    try {
      return CryptoServicesRegistrar.getSecureRandom();
    } catch (IllegalStateException e) {
      // Handle tests and/or local utilities that may not have initialized the FIPS provider by
      // calling registerFipsSecureRandom. Don't want to do this all the time because creating a new
      // SecureRandom every method call is extra work.
      registerFipsSecureRandom();
      return CryptoServicesRegistrar.getSecureRandom();
    }
  }
}
