package com.xgen.mongot.lifecycle;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Optional;
import org.bson.BsonDocument;

public class LifecycleConfig implements DocumentEncodable {

  static class Fields {
    public static final Field.WithDefault<Boolean> USE_LIFECYCLE_MANAGER =
        Field.builder("useLifecycleManager").booleanField().optional().withDefault(false);
    public static final Field.Optional<Integer> INITIALIZATION_THREADS =
        Field.builder("initializationThreads").intField().optional().noDefault();
  }

  public final boolean useLifecycleManager;
  public final Optional<Integer> initializationThreads;

  public LifecycleConfig(boolean useLifecycleManager, Optional<Integer> initializationThreads) {
    this.useLifecycleManager = useLifecycleManager;
    this.initializationThreads = initializationThreads;
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.USE_LIFECYCLE_MANAGER, this.useLifecycleManager)
        .field(Fields.INITIALIZATION_THREADS, this.initializationThreads)
        .build();
  }

  public static LifecycleConfig create(
      boolean useLifecycleManager, Optional<Integer> initializationThreads) {
    return new LifecycleConfig(useLifecycleManager, initializationThreads);
  }

  public static LifecycleConfig getDefault() {
    return new LifecycleConfig(true, Optional.empty());
  }
}
