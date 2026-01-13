package com.xgen.mongot.config.updater;

public interface ConfigUpdater extends AutoCloseable {

  void update();

  @Override
  void close();
}
