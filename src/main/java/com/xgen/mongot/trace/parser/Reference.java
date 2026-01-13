package com.xgen.mongot.trace.parser;

import com.fasterxml.jackson.annotation.JsonProperty;

class Reference {
  @SuppressWarnings("unused")
  @JsonProperty(value = "refType")
  private final String refType;

  @SuppressWarnings("unused")
  @JsonProperty(value = "spanID")
  private final String spanId;

  Reference(String refType, String spanId) {
    this.refType = refType;
    this.spanId = spanId;
  }
}
