package com.xgen.mongot.util.mongodb;

import com.google.errorprone.annotations.Var;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.Collection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;

/**
 * A class to assist with construction of SSLContexts.
 *
 * <p>Currently, services two disjoint use cases: Mongot Community, and OpenSSL dynamic linking.
 */
public class SslContextFactory {

  /**
   * Creates an SSLContext from a CA file containing multiple certificates.
   *
   * @param caFilePath Path to the CA file containing multiple certificates.
   * @return An initialized SSLContext.
   */
  public static SSLContext getWithCaFile(Path caFilePath) {
    Collection<? extends Certificate> certificates;
    try (InputStream caInput = new FileInputStream(caFilePath.toFile())) {
      CertificateFactory cf = CertificateFactory.getInstance("X.509");
      certificates = cf.generateCertificates(caInput);
    } catch (Exception e) {
      throw new RuntimeException("Failed to read certificates from CA file: " + caFilePath, e);
    }

    try {
      KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
      trustStore.load(null, null);

      @Var int certIndex = 1;
      for (var cert : certificates) {
        trustStore.setCertificateEntry("cert-" + certIndex, cert);
        certIndex++;
      }

      TrustManagerFactory tmf =
          TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      tmf.init(trustStore);

      SSLContext sslContext = SSLContext.getInstance("TLSv1.3");
      sslContext.init(null, tmf.getTrustManagers(), null);

      return sslContext;
    } catch (Exception e) {
      throw new RuntimeException("Failed to create SSL context from CA file", e);
    }
  }

  SslContext get() throws SslDynamicLinkingException {
    try {
      // try to dynamically link to system version of OpenSSL to provide better throughput to
      // mongod.
      return SslContextBuilder.forClient().sslProvider(SslProvider.OPENSSL).build();
    } catch (UnsatisfiedLinkError | SSLException e) {
      // OpenSSL provider may fail due to a linking problem. While this is not expected in
      // production, we failover to slower implementation if it does occur and will monitor
      // via log ingestion.
      throw new SslDynamicLinkingException(e);
    }
  }
}
