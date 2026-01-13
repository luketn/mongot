package com.xgen.mongot.index.definition;

import com.xgen.mongot.util.LoggableException;

public class InvalidViewDefinitionException extends LoggableException {

  private InvalidViewDefinitionException(String message) {
    super(message);
  }

  public static InvalidViewDefinitionException missingView(String viewName) {
    return new InvalidViewDefinitionException(
        String.format(
            "Cannot create or update index as the view '%s' was deleted or its "
                + "source collection has changed",
            viewName));
  }

  public static InvalidViewDefinitionException incompatiblePipeline(String error) {
    return new InvalidViewDefinitionException(
        "Cannot create or update index as the view pipeline is incompatible with Atlas Search: "
            + error);
  }
}
