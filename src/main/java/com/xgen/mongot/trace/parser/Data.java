package com.xgen.mongot.trace.parser;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import org.json.simple.JSONObject;

class Data {
  @SuppressWarnings("unused")
  @JsonProperty(value = "traceID")
  private final String traceId;

  @SuppressWarnings("unused")
  @JsonProperty(value = "spans")
  private final List<JaegerSpan> spans;

  @SuppressWarnings("unused")
  @JsonProperty(value = "processes")
  private final JSONObject processes;

  Data(String traceId, List<JaegerSpan> spans, JSONObject processes) {
    this.traceId = traceId;
    this.spans = spans;
    this.processes = processes;
  }
}
