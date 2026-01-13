package com.xgen.mongot.replication.mongodb.common;

import com.mongodb.session.ClientSession;

public interface RefreshingClientSession<S extends ClientSession> extends AutoCloseable {

  S getSession();

  @Override
  void close();
}
