package com.xgen.mongot.server.command.search.definition.request;

import com.google.common.collect.ImmutableList;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.List;

public class KillCursorsCommandDefinition {
  static class Fields {
    static final Field.Required<List<Long>> CURSOR_IDS =
        Field.builder("cursors").longField().mustBePositive().asList().required();
  }

  public static final String NAME = "killCursors";

  public final ImmutableList<Long> cursorIds;

  KillCursorsCommandDefinition(List<Long> cursorIds) {
    this.cursorIds = ImmutableList.copyOf(cursorIds);
  }

  public static KillCursorsCommandDefinition fromBson(DocumentParser parser)
      throws BsonParseException {
    return new KillCursorsCommandDefinition(parser.getField(Fields.CURSOR_IDS).unwrap());
  }
}
