package com.xgen.mongot.index.lucene.document;

import com.google.common.collect.ImmutableMap;
import com.xgen.mongot.index.IndexMetricsUpdater.IndexingMetricsUpdater;
import com.xgen.mongot.index.definition.DocumentFieldDefinition;
import com.xgen.mongot.index.definition.SearchFieldDefinitionResolver;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexFieldMapping;
import com.xgen.mongot.index.ingestion.stored.StoredBuilder;
import com.xgen.mongot.index.ingestion.stored.StoredDocumentBuilder;
import com.xgen.mongot.index.lucene.document.block.EmbeddedDocumentBuilder;
import com.xgen.mongot.index.lucene.document.builder.DocumentBlockBuilder;
import com.xgen.mongot.index.lucene.document.builder.DocumentBuilder;
import com.xgen.mongot.index.lucene.document.single.IndexableFieldFactory;
import com.xgen.mongot.index.lucene.document.single.LuceneSearchIndexDocumentBuilder;
import com.xgen.mongot.index.lucene.document.single.LuceneVectorIndexDocumentBuilder;
import com.xgen.mongot.index.lucene.document.single.RootDocumentBuilder;
import com.xgen.mongot.index.version.IndexCapabilities;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.Vector;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.facet.FacetsConfig;
import org.bson.types.ObjectId;

/**
 * Creates a {@link LuceneIndexingPolicy}, which in turn is able to instantiate {@link
 * DocumentBlockBuilder}s given the {@code byte[] id} of a given source document.
 */
public class DefaultIndexingPolicy {
  public static LuceneIndexingPolicy create(
      Analyzer indexAnalyzer,
      SearchFieldDefinitionResolver resolver,
      IndexingMetricsUpdater indexingMetricsUpdater) {
    return resolver.indexDefinition.hasEmbeddedFields()
        ? EmbeddedIndexingPolicy.create(indexAnalyzer, resolver, indexingMetricsUpdater)
        : RootDocumentIndexingPolicy.create(indexAnalyzer, resolver, indexingMetricsUpdater);
  }

  public static LuceneIndexingPolicy create(
      VectorIndexDefinition indexDefinition,
      IndexCapabilities indexCapabilities,
      IndexingMetricsUpdater indexingMetricsUpdater) {
    return VectorIndexDocumentIndexingPolicy.create(
        indexDefinition, indexCapabilities, indexingMetricsUpdater);
  }

  static class EmbeddedIndexingPolicy implements LuceneIndexingPolicy {
    private final RootDocumentIndexingPolicy rootDocumentIndexingPolicy;
    private final DocumentFieldDefinition mappings;
    private final IndexingMetricsUpdater indexingMetricsUpdater;

    private final Analyzer indexAnalyzer;
    private final SearchFieldDefinitionResolver resolver;

    EmbeddedIndexingPolicy(
        RootDocumentIndexingPolicy rootDocumentIndexingPolicy,
        DocumentFieldDefinition mappings,
        IndexingMetricsUpdater indexingMetricsUpdater,
        Analyzer indexAnalyzer,
        SearchFieldDefinitionResolver resolver) {
      this.rootDocumentIndexingPolicy = rootDocumentIndexingPolicy;
      this.mappings = mappings;
      this.indexingMetricsUpdater = indexingMetricsUpdater;
      this.indexAnalyzer = indexAnalyzer;
      this.resolver = resolver;
    }

    static EmbeddedIndexingPolicy create(
        Analyzer indexAnalyzer,
        SearchFieldDefinitionResolver resolver,
        IndexingMetricsUpdater indexingMetricsUpdater) {
      return new EmbeddedIndexingPolicy(
          RootDocumentIndexingPolicy.create(indexAnalyzer, resolver, indexingMetricsUpdater),
          resolver.indexDefinition.getMappings(),
          indexingMetricsUpdater,
          indexAnalyzer,
          resolver);
    }

    @Override
    public DocumentBlockBuilder createBuilder(byte[] id) {
      return EmbeddedDocumentBuilder.createRoot(
          this.rootDocumentIndexingPolicy.createBuilder(id),
          this.mappings,
          this.indexingMetricsUpdater,
          this.indexAnalyzer,
          this.resolver,
          id);
    }
  }

  public static class RootDocumentIndexingPolicy implements LuceneIndexingPolicy {
    private final Optional<FacetsConfig> facetsConfig;
    private final DocumentFieldDefinition mappings;
    private final Supplier<Optional<StoredBuilder>> storedBuilderFactory;
    private final boolean isEmbedded;

    private final Analyzer indexAnalyzer;
    private final SearchFieldDefinitionResolver resolver;
    private final IndexingMetricsUpdater indexingMetricsUpdater;
    private final ObjectId indexId;

    RootDocumentIndexingPolicy(
        ObjectId indexId,
        Optional<FacetsConfig> facetsConfig,
        DocumentFieldDefinition mappings,
        Supplier<Optional<StoredBuilder>> storedBuilderFactory,
        boolean isEmbedded,
        Analyzer indexAnalyzer,
        SearchFieldDefinitionResolver resolver,
        IndexingMetricsUpdater indexingMetricsUpdater) {
      this.indexId = indexId;
      this.facetsConfig = facetsConfig;
      this.mappings = mappings;
      this.storedBuilderFactory = storedBuilderFactory;
      this.isEmbedded = isEmbedded;
      this.indexAnalyzer = indexAnalyzer;
      this.resolver = resolver;
      this.indexingMetricsUpdater = indexingMetricsUpdater;
    }

    public static RootDocumentIndexingPolicy create(
        Analyzer indexAnalyzer,
        SearchFieldDefinitionResolver resolver,
        IndexingMetricsUpdater indexingMetricsUpdater) {
      return new RootDocumentIndexingPolicy(
          resolver.indexDefinition.getIndexId(),
          IndexableFieldFactory.createFacetsConfig(resolver.indexDefinition),
          resolver.indexDefinition.getMappings(),
          () -> StoredDocumentBuilder.create(resolver.indexDefinition.getStoredSource()),
          resolver.indexDefinition.hasEmbeddedFields(),
          indexAnalyzer,
          resolver,
          indexingMetricsUpdater);
    }

    @Override
    public RootDocumentBuilder createBuilder(byte[] id) {
      DocumentBuilder builder =
          this.isEmbedded
              ? LuceneSearchIndexDocumentBuilder.createEmbeddedRoot(
                  id, this.mappings, this.indexAnalyzer, this.resolver, this.indexingMetricsUpdater)
              : LuceneSearchIndexDocumentBuilder.createRoot(
                  id,
                  this.mappings,
                  this.indexAnalyzer,
                  this.resolver,
                  this.indexingMetricsUpdater);

      return RootDocumentBuilder.create(
          this.indexId,
          builder,
          this.storedBuilderFactory.get(),
          this.facetsConfig,
          this.resolver.getIndexCapabilities(),
          this.indexingMetricsUpdater);
    }
  }

  public static class VectorIndexDocumentIndexingPolicy implements LuceneIndexingPolicy {
    private final VectorIndexDefinition indexDefinition;
    private final VectorIndexFieldMapping mapping;
    private final IndexCapabilities indexCapabilities;
    private final IndexingMetricsUpdater indexingMetricsUpdater;

    VectorIndexDocumentIndexingPolicy(
        VectorIndexDefinition indexDefinition,
        VectorIndexFieldMapping mapping,
        IndexCapabilities indexCapabilities,
        IndexingMetricsUpdater indexingMetricsUpdater) {
      this.indexDefinition = indexDefinition;
      this.mapping = mapping;
      this.indexCapabilities = indexCapabilities;
      this.indexingMetricsUpdater = indexingMetricsUpdater;
    }

    public static VectorIndexDocumentIndexingPolicy create(
        VectorIndexDefinition indexDefinition,
        IndexCapabilities indexCapabilities,
        IndexingMetricsUpdater indexingMetricsUpdater) {
      return new VectorIndexDocumentIndexingPolicy(
          indexDefinition,
          indexDefinition.getMappings(),
          indexCapabilities,
          indexingMetricsUpdater);
    }

    @Override
    public DocumentBlockBuilder createBuilder(byte[] id) {
      return createBuilder(id, ImmutableMap.of());
    }

    @Override
    public DocumentBlockBuilder createBuilder(
        byte[] id, ImmutableMap<FieldPath, ImmutableMap<String, Vector>> autoEmbeddings) {
      LuceneVectorIndexDocumentBuilder builder =
          LuceneVectorIndexDocumentBuilder.createRoot(
              id,
              this.mapping,
              this.indexCapabilities,
              this.indexingMetricsUpdater,
              autoEmbeddings);

      Optional<StoredBuilder> storedBuilder =
          StoredDocumentBuilder.create(this.indexDefinition.getStoredSource());

      return RootDocumentBuilder.create(
          this.indexDefinition.getIndexId(),
          builder,
          storedBuilder,
          Optional.empty(),
          this.indexCapabilities,
          this.indexingMetricsUpdater);
    }
  }
}
