package com.xgen.mongot.index.lucene.directory;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.Field;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configuration class for environment variant performance settings. including -
 * enableByteReadInstrumentation This flag is used to enable or disable byte read instrumentation.
 */
public class EnvironmentVariantPerfConfig implements DocumentEncodable {
  private static final Logger LOG = LoggerFactory.getLogger(EnvironmentVariantPerfConfig.class);

  static class Fields {
    public static final Field.WithDefault<Boolean> ENABLE_BYTE_READ_INSTRUMENTATION =
        Field.builder("enableByteReadInstrumentation").booleanField().optional().withDefault(false);
  }

  private final boolean enableByteReadInstrumentation;

  public EnvironmentVariantPerfConfig(boolean enableByteReadInstrumentation) {
    LOG.info("enableByteReadInstrumentation is {}", enableByteReadInstrumentation);
    this.enableByteReadInstrumentation = enableByteReadInstrumentation;
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.ENABLE_BYTE_READ_INSTRUMENTATION, this.enableByteReadInstrumentation)
        .build();
  }

  public static EnvironmentVariantPerfConfig create(boolean enableByteReadInstrumentation) {
    return new EnvironmentVariantPerfConfig(enableByteReadInstrumentation);
  }

  public static EnvironmentVariantPerfConfig getDefault() {
    return new EnvironmentVariantPerfConfig(
        Fields.ENABLE_BYTE_READ_INSTRUMENTATION.getDefaultValue());
  }

  public boolean isByteReadInstrumentationEnabled() {
    return this.enableByteReadInstrumentation;
  }
}
