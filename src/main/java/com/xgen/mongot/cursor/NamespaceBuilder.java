package com.xgen.mongot.cursor;

import java.util.Optional;

public final class NamespaceBuilder {

  public static String build(String database, String collection, Optional<String> view) {
    return String.format("%s.%s", database, view.orElse(collection));
  }
}
