package com.xgen.mongot.server;

import java.io.Closeable;

public interface CommandServer extends Closeable {
  ServerStatus getServerStatus();

  enum ServerStatus {
    STARTED,
    NOT_STARTED
    // If needed, create an error status.
  }
}
