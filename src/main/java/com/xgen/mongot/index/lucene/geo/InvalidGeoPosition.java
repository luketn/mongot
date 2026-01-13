package com.xgen.mongot.index.lucene.geo;

import javax.annotation.Nullable;

public class InvalidGeoPosition extends Exception {

  public InvalidGeoPosition(@Nullable String message) {
    super(message);
  }
}
