package com.xgen.mongot.index.synonym;

import com.xgen.mongot.index.status.SynonymStatus;
import com.xgen.mongot.util.LoggableException;
import com.xgen.mongot.util.LoggableIdUtils;
import java.util.Optional;

/** Exceptions related to parsing synonym documents and building synonym artifacts. */
public class SynonymMappingException extends LoggableException {

  public enum Type {
    BUILD_ERROR,
    INVALID_DOCUMENT,
    UNKNOWN_MAPPING,
    MAPPING_NOT_READY,
  }

  public final Type type;

  SynonymMappingException(String message, Type type) {
    super(message);
    this.type = type;
  }

  SynonymMappingException(String message, Throwable throwable, Type type) {
    super(message, throwable);
    this.type = type;
  }

  public static SynonymMappingException failSynonymMapBuild(Throwable throwable) {
    return new SynonymMappingException("failed to build synonym map", throwable, Type.BUILD_ERROR);
  }

  /** An exception indicating an invalid document was found in a synonym source collection. */
  public static SynonymMappingException invalidSynonymDocument(
      Optional<String> id, Throwable throwable) {
    if (id.isEmpty()) {
      return invalidSynonymDocument(throwable);
    }
    String loggableId = LoggableIdUtils.getLoggableId(id);
    return new SynonymMappingException(
        String.format(
            "failed to analyze string in synonym document with _id %s: %s",
            loggableId, throwable.getMessage()),
        throwable,
        Type.INVALID_DOCUMENT);
  }

  public static SynonymMappingException invalidSynonymDocument(Throwable throwable) {
    return new SynonymMappingException(
        String.format("failed to analyze string in synonym document: %s", throwable.getMessage()),
        throwable,
        Type.INVALID_DOCUMENT);
  }

  public static SynonymMappingException unknownMappingName(String name) {
    return new SynonymMappingException(
        String.format("unknown synonym mapping name \"%s\"", name), Type.UNKNOWN_MAPPING);
  }

  /** A SynonymMappingException indicating that this mapping is not ready to service queries. */
  public static SynonymMappingException mappingNotReady(String name, SynonymStatus status) {
    return new SynonymMappingException(
        String.format(
            "synonym mapping \"%s\" is in state %s and is not ready to service queries",
            name, status.name()),
        Type.MAPPING_NOT_READY);
  }
}
