package com.xgen.mongot.server.command.search;

import com.xgen.mongot.cursor.MongotCursorManager;
import java.util.List;

public class CursorGuard implements AutoCloseable {
  private final List<Long> createdCursorIds;
  private final MongotCursorManager cursorManager;
  private final boolean populateCursor;

  CursorGuard(
      List<Long> createdCursorIds, MongotCursorManager cursorManager, boolean populateCursor) {
    this.createdCursorIds = createdCursorIds;
    this.cursorManager = cursorManager;
    this.populateCursor = populateCursor;
  }

  @Override
  public void close() throws Exception {
    if (!this.populateCursor) {
      this.createdCursorIds.forEach(this.cursorManager::killCursor);
    }
  }
}
