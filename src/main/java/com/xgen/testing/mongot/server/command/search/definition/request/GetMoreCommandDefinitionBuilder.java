package com.xgen.testing.mongot.server.command.search.definition.request;

import com.xgen.mongot.server.command.search.definition.request.BatchOptionsDefinition;
import com.xgen.mongot.server.command.search.definition.request.GetMoreCommandDefinition;
import com.xgen.mongot.util.Check;
import java.util.Optional;

public class GetMoreCommandDefinitionBuilder {

  private Optional<Long> cursorId = Optional.empty();

  private Optional<BatchOptionsDefinition> cursorOptions = Optional.empty();

  public static GetMoreCommandDefinitionBuilder builder() {
    return new GetMoreCommandDefinitionBuilder();
  }

  public GetMoreCommandDefinitionBuilder cursorId(long cursorId) {
    this.cursorId = Optional.of(cursorId);
    return this;
  }

  public GetMoreCommandDefinitionBuilder cursorOptions(BatchOptionsDefinition cursorOptions) {
    this.cursorOptions = Optional.of(cursorOptions);
    return this;
  }

  /** Builds the GetMoreCommandDefinition. */
  public GetMoreCommandDefinition build() {
    Check.isPresent(this.cursorId, "cursorId");

    return new GetMoreCommandDefinition(this.cursorId.get(), this.cursorOptions);
  }
}
