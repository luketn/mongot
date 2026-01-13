package com.xgen.mongot.config.provider.community;

import com.xgen.mongot.config.provider.community.embedding.EmbeddingConfig;
import com.xgen.mongot.util.bson.YamlCodec;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public record CommunityConfig(
    SyncSourceConfig syncSourceConfig,
    StorageConfig storageConfig,
    ServerConfig serverConfig,
    FtdcCommunityConfig ftdcConfig,
    Optional<MetricsConfig> metricsConfig,
    Optional<HealthCheckConfig> healthCheckConfig,
    Optional<LoggingConfig> loggingConfig,
    Optional<EmbeddingConfig> embeddingConfig)
    implements DocumentEncodable {

  private static final Logger LOG = LoggerFactory.getLogger(CommunityConfig.class);

  private static class Fields {
    public static final Field.Required<SyncSourceConfig> SYNC_SOURCE =
        Field.builder("syncSource")
            .classField(SyncSourceConfig::fromBson)
            .disallowUnknownFields()
            .required();

    public static final Field.Required<StorageConfig> STORAGE =
        Field.builder("storage")
            .classField(StorageConfig::fromBson)
            .disallowUnknownFields()
            .required();

    public static final Field.Required<ServerConfig> SERVER =
        Field.builder("server")
            .classField(ServerConfig::fromBson)
            .disallowUnknownFields()
            .required();

    public static final Field.WithDefault<FtdcCommunityConfig> FTDC =
        Field.builder("ftdc")
            .classField(FtdcCommunityConfig::fromBson)
            .disallowUnknownFields()
            .optional()
            .withDefault(FtdcCommunityConfig.getDefault());

    public static final Field.Optional<MetricsConfig> METRICS =
        Field.builder("metrics")
            .classField(MetricsConfig::fromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();

    public static final Field.Optional<HealthCheckConfig> HEALTH_CHECK =
        Field.builder("healthCheck")
            .classField(HealthCheckConfig::fromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();

    public static final Field.Optional<LoggingConfig> LOGGING =
        Field.builder("logging")
            .classField(LoggingConfig::fromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();

    public static final Field.Optional<EmbeddingConfig> EMBEDDING =
        Field.builder("embedding")
            .classField(EmbeddingConfig::fromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();
  }

  public static CommunityConfig readFromFile(Path configPath)
      throws IOException, BsonParseException {
    LOG.atInfo().addKeyValue("configPath", configPath).log("Reading config from file");
    String yaml = Files.readString(configPath);
    BsonDocument bson = YamlCodec.fromYaml(yaml);
    return CommunityConfig.fromBson(bson);
  }

  private static CommunityConfig fromBson(BsonDocument document) throws BsonParseException {
    try (var parser = BsonDocumentParser.fromRoot(document).allowUnknownFields(true).build()) {
      return fromBson(parser);
    }
  }

  public static CommunityConfig fromBson(DocumentParser parser) throws BsonParseException {
    return new CommunityConfig(
        parser.getField(Fields.SYNC_SOURCE).unwrap(),
        parser.getField(Fields.STORAGE).unwrap(),
        parser.getField(Fields.SERVER).unwrap(),
        parser.getField(Fields.FTDC).unwrap(),
        parser.getField(Fields.METRICS).unwrap(),
        parser.getField(Fields.HEALTH_CHECK).unwrap(),
        parser.getField(Fields.LOGGING).unwrap(),
        parser.getField(Fields.EMBEDDING).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.SYNC_SOURCE, this.syncSourceConfig)
        .field(Fields.STORAGE, this.storageConfig)
        .field(Fields.SERVER, this.serverConfig)
        .field(Fields.FTDC, this.ftdcConfig)
        .field(Fields.METRICS, this.metricsConfig)
        .field(Fields.HEALTH_CHECK, this.healthCheckConfig)
        .field(Fields.LOGGING, this.loggingConfig)
        .field(Fields.EMBEDDING, this.embeddingConfig)
        .build();
  }
}
