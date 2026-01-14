package com.xgen.mongot.embedding.providers.configs;

import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig.EmbeddingProvider;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig.VoyageEmbeddingCredentials;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig.VoyageModelConfig;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;

public class EmbeddingConfigFactory {
  private static class Fields {
    public static final Field.Required<String> EMBEDDING_PROVIDER =
        Field.builder("embeddingProvider").stringField().mustNotBeEmpty().required();
    public static final Field.Required<String> MODEL_NAME =
        Field.builder("modelName").stringField().mustNotBeEmpty().required();
    public static final Field.Required<EmbeddingServiceConfig.EmbeddingConfig> CONFIG =
        Field.builder("config")
            .classField(EmbeddingServiceConfig.EmbeddingConfig::fromBson)
            .allowUnknownFields()
            .required();
    public static final Field.Required<String> PROVIDER =
        Field.builder("_provider").stringField().mustNotBeEmpty().required();
  }

  public static EmbeddingServiceConfig fromBson(DocumentParser parser) throws BsonParseException {
    EmbeddingProvider provider =
        EmbeddingProvider.valueOf(parser.getField(Fields.EMBEDDING_PROVIDER).unwrap());
    String model = parser.getField(Fields.MODEL_NAME).unwrap().toLowerCase();
    EmbeddingServiceConfig.EmbeddingConfig config = parser.getField(Fields.CONFIG).unwrap();
    // TODO(CLOUDP-373068): Parse compatibility params from ConfCall.
    return new EmbeddingServiceConfig(provider, model, config);
  }

  public static EmbeddingServiceConfig.EmbeddingCredentials getCredentials(DocumentParser parser)
      throws BsonParseException {
    EmbeddingProvider provider =
        EmbeddingProvider.valueOf(parser.getField(Fields.PROVIDER).unwrap());
    switch (provider) {
      case VOYAGE -> {
        return VoyageEmbeddingCredentials.fromBson(parser);
      }
      default -> throw new IllegalStateException("Unsupported provider: " + provider);
    }
  }

  public static EmbeddingServiceConfig.ModelConfig getModelConfig(DocumentParser parser)
      throws BsonParseException {
    EmbeddingProvider provider =
        EmbeddingProvider.valueOf(parser.getField(Fields.PROVIDER).unwrap());
    switch (provider) {
      case VOYAGE -> {
        return VoyageModelConfig.fromBson(parser);
      }
      default -> throw new IllegalStateException("Unsupported provider: " + provider);
    }
  }
}
