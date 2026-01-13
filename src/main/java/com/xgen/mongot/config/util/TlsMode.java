package com.xgen.mongot.config.util;

import java.util.Optional;

public enum TlsMode {
  TLS("tls"),
  MTLS("mtls"),
  DISABLED("disabled");
  private final String value;

  TlsMode(String value) {
    this.value = value;
  }

  public static Optional<TlsMode> fromString(String mode) {
    for (TlsMode tlsMode : TlsMode.values()) {
      if (tlsMode.value.equalsIgnoreCase(mode)) {
        return Optional.of(tlsMode);
      }
    }
    return Optional.empty();
  }
}
