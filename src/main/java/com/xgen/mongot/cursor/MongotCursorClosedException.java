package com.xgen.mongot.cursor;

import com.xgen.mongot.util.LoggableException;

/**
 * A MongotCursorClosedException is thrown when a cursor that has already been closed is attempting
 * to be read from.
 */
public class MongotCursorClosedException extends LoggableException {

  public MongotCursorClosedException(Long cursorId) {
    super("cursorId: " + cursorId);
  }
}
