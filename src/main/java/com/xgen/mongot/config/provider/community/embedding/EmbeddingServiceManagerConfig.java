package com.xgen.mongot.config.provider.community.embedding;

import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig.VoyageEmbeddingCredentials;
import com.xgen.mongot.util.bson.YamlCodec;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Configuration for embedding service manager in community edition. */
public record EmbeddingServiceManagerConfig(List<EmbeddingServiceConfig> configs)
    implements DocumentEncodable {

  private static final Logger LOG = LoggerFactory.getLogger(EmbeddingServiceManagerConfig.class);
  private static final String CONFIG_RESOURCE = "config/community/embedding-service-configs.yml";

  /** Simple holder for Voyage API credentials. */
  public record VoyageCredentials(
      VoyageEmbeddingCredentials queryCredentials,
      VoyageEmbeddingCredentials indexingCredentials) {}

  private static class Fields {
    public static final Field.Required<List<BsonDocument>> CONFIGS =
        Field.builder("configs").documentField().asList().required();
  }

  /**
   * Loads embedding configuration from the internal classpath resource.
   *
   * @param credentials the Voyage API credentials (query and indexing keys)
   * @return optional embedding service manager configuration, empty if credentials not provided
   */
  public static Optional<EmbeddingServiceManagerConfig> loadEmbeddingServiceConfig(
      Optional<VoyageCredentials> credentials) {

    if (credentials.isEmpty()) {
      return Optional.empty();
    }

    try (InputStream resourceStream =
        EmbeddingServiceManagerConfig.class.getClassLoader().getResourceAsStream(CONFIG_RESOURCE)) {

      // Shouldn't ever happen since resources are built with this resource
      if (resourceStream == null) {
        LOG.info(
            "No embedding configuration found at {}. Auto-embedding functionality will be disabled",
            CONFIG_RESOURCE);
        return Optional.empty();
      }

      LOG.atInfo()
          .addKeyValue("resourcePath", CONFIG_RESOURCE)
          .log("Reading embedding configuration from internal resource");

      // YAML → BSON
      BsonDocument bson =
          YamlCodec.fromYaml(new String(resourceStream.readAllBytes(), StandardCharsets.UTF_8));
      // BSON → EmbeddingServiceManagerConfig with injected credentials
      return Optional.of(fromBson(bson, credentials.get()));
    } catch (IOException e) {
      LOG.error("Failed to read embedding configuration resource", e);
      return Optional.empty();
    } catch (BsonParseException e) {
      LOG.error("Failed to parse embedding configuration", e);
      return Optional.empty();
    }
  }

  /**
   * Helper method to parse BSON document (follows CommunityConfig pattern).
   *
   * @param document the BSON document
   * @param credentials the Voyage credentials to inject
   * @return the embedding service manager configuration
   * @throws BsonParseException if parsing fails
   */
  private static EmbeddingServiceManagerConfig fromBson(
      BsonDocument document, VoyageCredentials credentials) throws BsonParseException {
    try (var parser = BsonDocumentParser.fromRoot(document).build()) {
      return fromBson(parser, credentials);
    }
  }

  /**
   * Parses embedding configuration from BSON and injects provided credentials.
   *
   * <p>No validation is performed since the internal YAML file is MongoDB-controlled and trusted.
   *
   * @param parser the document parser
   * @param credentials the Voyage credentials to inject
   * @return the embedding service manager configuration
   * @throws BsonParseException if parsing fails
   */
  private static EmbeddingServiceManagerConfig fromBson(
      DocumentParser parser, VoyageCredentials credentials) throws BsonParseException {

    List<BsonDocument> configDocs = parser.getField(Fields.CONFIGS).unwrap();

    List<EmbeddingServiceConfig> serviceConfigs = new ArrayList<>();
    for (BsonDocument configDoc : configDocs) {
      // Inject credentials into each config
      injectCredentials(configDoc, credentials);

      // Parse the modified document
      try (DocumentParser configParser = BsonDocumentParser.fromRoot(configDoc).build()) {
        serviceConfigs.add(EmbeddingServiceConfig.fromBson(configParser));
      }
    }

    return new EmbeddingServiceManagerConfig(serviceConfigs);
  }

  /**
   * Injects Voyage API credentials and community-specific fields into the config document.
   *
   * <p>Injects: - _provider discriminator into modelConfig - Base credentials (indexing key) for
   * default operations - Query workload credentials (query key) for query operations -
   * isDedicatedCluster: true
   */
  private static void injectCredentials(BsonDocument configDoc, VoyageCredentials credentials) {
    if (!configDoc.containsKey("config")) {
      return;
    }
    BsonDocument configField = configDoc.getDocument("config");

    // Inject _provider into base modelConfig
    if (configField.containsKey("modelConfig")) {
      BsonDocument modelConfig = configField.getDocument("modelConfig");
      if (!modelConfig.containsKey("_provider")) {
        modelConfig.put("_provider", new BsonString("VOYAGE"));
      }
    }

    // Inject base level credentials
    if (!configField.containsKey("credentials")) {
      BsonDocument credentialsDoc = credentials.indexingCredentials.toBson();
      credentialsDoc.put("_provider", new BsonString("VOYAGE"));
      configField.put("credentials", credentialsDoc);
    }

    // Inject query workload credentials override
    if (!configField.containsKey("query")) {
      BsonDocument queryCredentialsDoc = credentials.queryCredentials.toBson();
      queryCredentialsDoc.put("_provider", new BsonString("VOYAGE"));

      BsonDocument queryParams = new BsonDocument();
      queryParams.put("credentials", queryCredentialsDoc);

      configField.put("query", queryParams);
    }

    // Inject isDedicatedCluster for each config
    if (!configField.containsKey("isDedicatedCluster")) {
      configField.put("isDedicatedCluster", BsonBoolean.TRUE);
    }
  }

  @Override
  public BsonDocument toBson() {
    List<BsonDocument> configDocs = new ArrayList<>();
    for (EmbeddingServiceConfig config : this.configs) {
      configDocs.add(config.toBson());
    }
    Field.Required<List<BsonDocument>> configsField =
        Field.builder("configs").documentField().asList().required();
    return BsonDocumentBuilder.builder().field(configsField, configDocs).build();
  }
}
