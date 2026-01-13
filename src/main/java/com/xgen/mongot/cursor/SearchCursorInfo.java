package com.xgen.mongot.cursor;

import com.xgen.mongot.index.MetaResults;

public class SearchCursorInfo {
  public final long cursorId;
  public final MetaResults metaResults;

  public SearchCursorInfo(long cursorId, MetaResults metaResults) {
    this.cursorId = cursorId;
    this.metaResults = metaResults;
  }
}
