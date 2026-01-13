package com.xgen.mongot.trace.parser;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

class Process {
  @SuppressWarnings("unused")
  @JsonProperty(value = "serviceName")
  private final String serviceName;

  @SuppressWarnings("unused")
  @JsonProperty(value = "tags")
  private final List<Tag> tags;

  Process(String serviceName, List<Tag> tags) {
    this.serviceName = serviceName;
    this.tags = tags;
  }
}
