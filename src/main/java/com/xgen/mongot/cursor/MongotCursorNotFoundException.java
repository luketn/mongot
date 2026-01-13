package com.xgen.mongot.cursor;

import com.xgen.mongot.util.LoggableException;

/**
 * A MongotCursorNotFoundException is thrown when a CursorId cannot be found in the
 * MongotCursorManager.
 */
public class MongotCursorNotFoundException extends LoggableException {

  public MongotCursorNotFoundException(Long cursorId) {
    super("cursorId: " + cursorId);
  }
}
