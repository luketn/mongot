package com.xgen.mongot.embedding.providers.clients;

import static com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig.EmbeddingProvider;
import static com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig.ServiceTier;

import com.xgen.mongot.embedding.MongotMetadata;
import com.xgen.mongot.embedding.providers.configs.EmbeddingModelConfig;
import com.xgen.mongot.metrics.MetricsFactory;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import java.util.Optional;

/**
 * EmbeddingClientFactory will be used to create a variety of clients to be managed by
 * EmbeddingServiceManager
 */
public class EmbeddingClientFactory {

  private final MeterRegistry meterRegistry;
  private final Optional<MongotMetadata> mongotMetadata;

  public EmbeddingClientFactory(MeterRegistry meterRegistry) {
    this.meterRegistry = meterRegistry;
    this.mongotMetadata = Optional.empty();
  }

  /**
   * Constructor for bootstrappers with metadata.
   *
   * @param meterRegistry the meter registry for metrics
   * @param metadata mongot metadata
   */
  public EmbeddingClientFactory(MeterRegistry meterRegistry,
      Optional<MongotMetadata> metadata) {
    this.meterRegistry = meterRegistry;
    this.mongotMetadata = metadata;
  }

  public ClientInterface createEmbeddingClient(
      EmbeddingModelConfig embeddingModelConfig,
      ServiceTier serviceTier,
      EmbeddingModelConfig.ConsolidatedWorkloadParams workloadParams) {
    EmbeddingProvider provider = embeddingModelConfig.provider();
    MetricsFactory metricsFactory =
        new MetricsFactory(
            "embeddingClient",
            this.meterRegistry,
            Tags.of(
                Tag.of("provider", provider.name()),
                Tag.of("canonicalModel", embeddingModelConfig.name()),
                Tag.of("workload", serviceTier.name())));
    return switch (provider) {
      case EmbeddingProvider.AWS_BEDROCK, EmbeddingProvider.COHERE ->
          throw new UnsupportedOperationException("Unsupported cloud provider: " + provider);
      case EmbeddingProvider.VOYAGE ->
          new VoyageClient(
              embeddingModelConfig,
              serviceTier,
              workloadParams,
              metricsFactory,
              this.mongotMetadata);
    };
  }
}
