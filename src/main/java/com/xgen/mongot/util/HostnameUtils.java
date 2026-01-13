package com.xgen.mongot.util;

public class HostnameUtils {
  public static String getSimplifiedHostname(String host) {
    // For now, use hostname without domain.
    if (host != null && host.contains(".")) {
      return host.substring(0, host.indexOf('.'));
    }
    return host;
  }
}
