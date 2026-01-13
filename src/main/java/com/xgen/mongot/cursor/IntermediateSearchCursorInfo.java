package com.xgen.mongot.cursor;

public class IntermediateSearchCursorInfo {
  public final long searchCursorId;
  public final long metaCursorId;

  public IntermediateSearchCursorInfo(long searchCursorId, long metaCursorId) {
    this.searchCursorId = searchCursorId;
    this.metaCursorId = metaCursorId;
  }
}
