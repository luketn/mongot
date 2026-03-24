package com.xgen.mongot.index.autoembedding;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.xgen.mongot.embedding.providers.configs.EmbeddingModelCatalog;
import com.xgen.mongot.embedding.providers.configs.EmbeddingModelConfig;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig;
import com.xgen.mongot.index.definition.VectorAutoEmbedFieldDefinition;
import com.xgen.mongot.index.definition.VectorFieldSpecification;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexFieldDefinition;
import com.xgen.mongot.index.definition.VectorIndexFilterFieldDefinition;
import com.xgen.mongot.index.definition.VectorQuantization;
import com.xgen.mongot.index.definition.VectorSimilarity;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import java.util.List;
import java.util.Optional;
import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.Test;

/** Unit tests for {@link MaterializedViewIndexGenerationUtil#skipInitialSync}. */
public class MaterializedViewIndexGenerationUtilTest {

  private static final ObjectId INDEX_ID = new ObjectId();

  @Before
  public void setupRegistry() {
    EmbeddingModelCatalog.registerModelConfig(
        "voyage-3-large",
        EmbeddingModelConfig.create(
            "voyage-3-large",
            EmbeddingServiceConfig.EmbeddingProvider.VOYAGE,
            new EmbeddingServiceConfig.EmbeddingConfig(
                Optional.of("us-east-1"),
                new EmbeddingServiceConfig.VoyageModelConfig(
                    Optional.of(512),
                    Optional.of(EmbeddingServiceConfig.TruncationOption.START),
                    Optional.of(100),
                    Optional.of(1000)),
                new EmbeddingServiceConfig.ErrorHandlingConfig(50, 50L, 10L, 0.1),
                new EmbeddingServiceConfig.VoyageEmbeddingCredentials(
                    "token123", "2024-10-15T22:32:20.925Z"),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                true,
                Optional.empty())));
  }

  @Test
  public void skipInitialSync_sameDefinitionDifferentOrder_returnsTrue() {
    VectorIndexFieldDefinition autoEmbedFieldDefinition =
        new VectorAutoEmbedFieldDefinition(
            "voyage-3-large",
            "text",
            FieldPath.parse("desc"),
            VectorSimilarity.DOT_PRODUCT,
            VectorQuantization.NONE,
            Optional.empty());
    VectorIndexDefinition def1 =
        VectorIndexDefinitionBuilder.builder()
            .indexId(INDEX_ID)
            .withDefinitionVersion(Optional.of(1L))
            .setFields(
                List.of(
                    autoEmbedFieldDefinition,
                    new VectorIndexFilterFieldDefinition(FieldPath.parse("category"))))
            .build();
    VectorIndexDefinition def2 =
        VectorIndexDefinitionBuilder.builder()
            .indexId(INDEX_ID)
            .withDefinitionVersion(Optional.of(1L))
            .setFields(
                List.of(
                    new VectorIndexFilterFieldDefinition(FieldPath.parse("category")),
                    autoEmbedFieldDefinition))
            .build();
    assertTrue(MaterializedViewIndexGenerationUtil.skipInitialSync(def1, def2));
  }

  @Test
  public void skipInitialSync_onlyIncreaseDefinitionVersion_returnsFalse() {
    List<VectorIndexFieldDefinition> fields =
        List.of(
            new VectorAutoEmbedFieldDefinition(
                "voyage-3-large",
                "text",
                FieldPath.parse("desc"),
                VectorSimilarity.DOT_PRODUCT,
                VectorQuantization.NONE,
                Optional.empty()),
            new VectorIndexFilterFieldDefinition(FieldPath.parse("category")));
    VectorIndexDefinition defVersion1 =
        VectorIndexDefinitionBuilder.builder()
            .indexId(INDEX_ID)
            .withDefinitionVersion(Optional.of(1L))
            .setFields(fields)
            .build();
    VectorIndexDefinition defVersion2 =
        VectorIndexDefinitionBuilder.builder()
            .indexId(INDEX_ID)
            .withDefinitionVersion(Optional.of(2L))
            .setFields(fields)
            .build();
    assertFalse(MaterializedViewIndexGenerationUtil.skipInitialSync(defVersion1, defVersion2));
  }

  @Test
  public void skipInitialSync_redefineHnswOptions_returnsTrue() {
    VectorIndexDefinition def1 =
        VectorIndexDefinitionBuilder.builder()
            .indexId(INDEX_ID)
            .withDefinitionVersion(Optional.of(1L))
            .setFields(
                List.of(
                    new VectorAutoEmbedFieldDefinition(
                        "voyage-3-large",
                        "text",
                        FieldPath.parse("desc"),
                        VectorSimilarity.DOT_PRODUCT,
                        VectorQuantization.NONE,
                        Optional.empty()),
                    new VectorIndexFilterFieldDefinition(FieldPath.parse("category"))))
            .build();
    VectorIndexDefinition def2 =
        VectorIndexDefinitionBuilder.builder()
            .indexId(INDEX_ID)
            .withDefinitionVersion(Optional.of(2L))
            .setFields(
                List.of(
                    new VectorAutoEmbedFieldDefinition(
                        "voyage-3-large",
                        "text",
                        FieldPath.parse("desc"),
                        VectorSimilarity.DOT_PRODUCT,
                        VectorQuantization.NONE,
                        Optional.of(new VectorFieldSpecification.HnswOptions(32, 200))),
                    new VectorIndexFilterFieldDefinition(FieldPath.parse("category"))))
            .build();
    assertTrue(MaterializedViewIndexGenerationUtil.skipInitialSync(def1, def2));
    assertTrue(MaterializedViewIndexGenerationUtil.skipInitialSync(def2, def1));
  }

  @Test
  public void skipInitialSync_redefinePath_returnsFalse() {
    VectorIndexDefinition def1 =
        VectorIndexDefinitionBuilder.builder()
            .indexId(INDEX_ID)
            .withDefinitionVersion(Optional.of(1L))
            .setFields(
                List.of(
                    new VectorAutoEmbedFieldDefinition(
                        "voyage-3-large",
                        "text",
                        FieldPath.parse("desc"),
                        VectorSimilarity.DOT_PRODUCT,
                        VectorQuantization.NONE,
                        Optional.empty()),
                    new VectorIndexFilterFieldDefinition(FieldPath.parse("category"))))
            .build();
    VectorIndexDefinition def2 =
        VectorIndexDefinitionBuilder.builder()
            .indexId(INDEX_ID)
            .withDefinitionVersion(Optional.of(1L))
            .setFields(
                List.of(
                    new VectorAutoEmbedFieldDefinition(
                        "voyage-3-large",
                        "text",
                        FieldPath.parse("title"),
                        VectorSimilarity.DOT_PRODUCT,
                        VectorQuantization.NONE,
                        Optional.empty()),
                    new VectorIndexFilterFieldDefinition(FieldPath.parse("category"))))
            .build();
    assertFalse(MaterializedViewIndexGenerationUtil.skipInitialSync(def1, def2));
  }

  @Test
  public void skipInitialSync_redefineFilterField_returnsFalse() {
    VectorIndexFieldDefinition autoEmbedFieldDefinition =
        new VectorAutoEmbedFieldDefinition(
            "voyage-3-large",
            "text",
            FieldPath.parse("desc"),
            VectorSimilarity.DOT_PRODUCT,
            VectorQuantization.NONE,
            Optional.empty());
    VectorIndexDefinition def1 =
        VectorIndexDefinitionBuilder.builder()
            .indexId(INDEX_ID)
            .withDefinitionVersion(Optional.of(1L))
            .setFields(
                List.of(
                    autoEmbedFieldDefinition,
                    new VectorIndexFilterFieldDefinition(FieldPath.parse("category"))))
            .build();
    VectorIndexDefinition def2 =
        VectorIndexDefinitionBuilder.builder()
            .indexId(INDEX_ID)
            .withDefinitionVersion(Optional.of(1L))
            .setFields(
                List.of(
                    autoEmbedFieldDefinition,
                    new VectorIndexFilterFieldDefinition(FieldPath.parse("genre"))))
            .build();
    assertFalse(MaterializedViewIndexGenerationUtil.skipInitialSync(def1, def2));
  }

  @Test
  public void skipInitialSync_onlyIncreaseNumPartitions_returnsTrue() {
    List<VectorIndexFieldDefinition> fields =
        List.of(
            new VectorAutoEmbedFieldDefinition(
                "voyage-3-large",
                "text",
                FieldPath.parse("desc"),
                VectorSimilarity.DOT_PRODUCT,
                VectorQuantization.NONE,
                Optional.empty()),
            new VectorIndexFilterFieldDefinition(FieldPath.parse("category")));
    VectorIndexDefinition def1 =
        VectorIndexDefinitionBuilder.builder()
            .indexId(INDEX_ID)
            .withDefinitionVersion(Optional.of(1L))
            .numPartitions(2)
            .setFields(fields)
            .build();
    VectorIndexDefinition def2 =
        VectorIndexDefinitionBuilder.builder()
            .indexId(INDEX_ID)
            .withDefinitionVersion(Optional.of(2L))
            .numPartitions(3)
            .setFields(fields)
            .build();
    assertTrue(MaterializedViewIndexGenerationUtil.skipInitialSync(def1, def2));
    assertTrue(MaterializedViewIndexGenerationUtil.skipInitialSync(def2, def1));
  }

  @Test
  public void skipInitialSync_firstAutoEmbedLuceneOnlySecondQuantization_returnsFalse() {
    VectorIndexFieldDefinition filter =
        new VectorIndexFilterFieldDefinition(FieldPath.parse("category"));
    VectorIndexDefinition def1 =
        VectorIndexDefinitionBuilder.builder()
            .indexId(INDEX_ID)
            .withDefinitionVersion(Optional.of(1L))
            .setFields(
                List.of(
                    new VectorAutoEmbedFieldDefinition(
                        "voyage-3-large",
                        "text",
                        FieldPath.parse("desc"),
                        VectorSimilarity.DOT_PRODUCT,
                        VectorQuantization.NONE,
                        Optional.empty()),
                    new VectorAutoEmbedFieldDefinition(
                        "voyage-3-large",
                        "text",
                        FieldPath.parse("title"),
                        VectorSimilarity.DOT_PRODUCT,
                        VectorQuantization.NONE,
                        Optional.empty()),
                    filter))
            .build();
    VectorIndexDefinition def2 =
        VectorIndexDefinitionBuilder.builder()
            .indexId(INDEX_ID)
            .withDefinitionVersion(Optional.of(2L))
            .setFields(
                List.of(
                    new VectorAutoEmbedFieldDefinition(
                        "voyage-3-large",
                        "text",
                        FieldPath.parse("desc"),
                        VectorSimilarity.DOT_PRODUCT,
                        VectorQuantization.NONE,
                        Optional.of(new VectorFieldSpecification.HnswOptions(32, 200))),
                    new VectorAutoEmbedFieldDefinition(
                        "voyage-3-large",
                        "text",
                        FieldPath.parse("title"),
                        VectorSimilarity.DOT_PRODUCT,
                        VectorQuantization.SCALAR,
                        Optional.empty()),
                    filter))
            .build();
    assertFalse(MaterializedViewIndexGenerationUtil.skipInitialSync(def1, def2));
  }

  @Test
  public void skipInitialSync_onlyIncreaseIndexFeatureVersion_returnsTrue() {
    List<VectorIndexFieldDefinition> fields =
        List.of(
            new VectorAutoEmbedFieldDefinition(
                "voyage-3-large",
                "text",
                FieldPath.parse("desc"),
                VectorSimilarity.DOT_PRODUCT,
                VectorQuantization.NONE,
                Optional.empty()),
            new VectorIndexFilterFieldDefinition(FieldPath.parse("category")));
    VectorIndexDefinition def1 =
        VectorIndexDefinitionBuilder.builder()
            .indexId(INDEX_ID)
            .withDefinitionVersion(Optional.of(1L))
            .indexFeatureVersion(2)
            .setFields(fields)
            .build();
    VectorIndexDefinition def2 =
        VectorIndexDefinitionBuilder.builder()
            .indexId(INDEX_ID)
            .withDefinitionVersion(Optional.of(2L))
            .indexFeatureVersion(3)
            .setFields(fields)
            .build();
    assertTrue(MaterializedViewIndexGenerationUtil.skipInitialSync(def1, def2));
    assertTrue(MaterializedViewIndexGenerationUtil.skipInitialSync(def2, def1));
  }
}
