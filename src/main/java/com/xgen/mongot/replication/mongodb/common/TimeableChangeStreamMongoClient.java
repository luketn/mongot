package com.xgen.mongot.replication.mongodb.common;

import java.time.Duration;

/** A {@link ChangeStreamMongoClient} which tracks the duration since being created. */
public interface TimeableChangeStreamMongoClient<E extends Exception>
    extends ChangeStreamMongoClient<E> {

  Duration getUptime();
}
