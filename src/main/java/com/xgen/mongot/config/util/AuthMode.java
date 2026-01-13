package com.xgen.mongot.config.util;

import java.util.Optional;

public enum AuthMode {
  KEYFILE("keyfile"),
  DISABLED("disabled");
  private final String value;

  AuthMode(String value) {
    this.value = value;
  }

  public static Optional<AuthMode> fromString(String mode) {
    for (AuthMode authMode : AuthMode.values()) {
      if (authMode.value.equalsIgnoreCase(mode)) {
        return Optional.of(authMode);
      }
    }
    return Optional.empty();
  }
}
