package com.xgen.mongot.trace.parser;

import com.fasterxml.jackson.annotation.JsonProperty;

class Tag {
  @SuppressWarnings("unused")
  @JsonProperty(value = "key")
  private final String key;

  @SuppressWarnings("unused")
  @JsonProperty(value = "type")
  private final String type;

  @SuppressWarnings("unused")
  @JsonProperty(value = "value")
  private final String value;

  Tag(String key, String type, String value) {
    this.key = key;
    this.type = type;
    this.value = value;
  }
}
