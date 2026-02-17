package com.xgen.mongot.embedding.utils;

import static com.xgen.mongot.embedding.utils.ReplaceStringsFieldValueHandler.HASH_FIELD_SUFFIX;
import static com.xgen.mongot.index.mongodb.MaterializedViewWriter.MV_DATABASE_NAME;

import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata;
import com.xgen.mongot.index.definition.VectorAutoEmbedFieldDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexFieldDefinition;
import com.xgen.mongot.index.definition.VectorIndexFilterFieldDefinition;
import com.xgen.mongot.index.definition.VectorQuantization;
import com.xgen.mongot.index.definition.VectorSimilarity;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.Assert;
import org.junit.Test;

public class AutoEmbeddingIndexDefinitionUtilsTest {
  private static final MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata
      MAT_VIEW_SCHEMA_METADATA =
          new MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata(0, Map.of());

  @Test
  public void testGetDerivedVectorIndexDefinition_version0() {

    var defaultAutoEmbedField = new VectorAutoEmbedFieldDefinition(FieldPath.parse("a"));
    var autoEmbedFieldWithSpecifications =
        new VectorAutoEmbedFieldDefinition(
            "voyage-3-large",
            "text",
            FieldPath.parse("b"),
            VectorSimilarity.COSINE,
            VectorQuantization.NONE);
    var filterField = new VectorIndexFilterFieldDefinition(FieldPath.parse("color"));

    List<VectorIndexFieldDefinition> fields =
        List.of(defaultAutoEmbedField, autoEmbedFieldWithSpecifications, filterField);
    var autoEmbedIndexDefinition = VectorIndexDefinitionBuilder.builder().setFields(fields).build();

    var collectionUuid = UUID.randomUUID();
    var derivedIndexDefinition =
        AutoEmbeddingIndexDefinitionUtils.getDerivedVectorIndexDefinition(
            autoEmbedIndexDefinition, MV_DATABASE_NAME, collectionUuid, MAT_VIEW_SCHEMA_METADATA);

    Assert.assertEquals(collectionUuid, derivedIndexDefinition.getCollectionUuid());
    Assert.assertEquals(
        autoEmbedIndexDefinition.getFields().size(), derivedIndexDefinition.getFields().size());

    var defaultVectorField = getVectorFieldDefinition("a", derivedIndexDefinition);
    Assert.assertEquals(VectorIndexFieldDefinition.Type.VECTOR, defaultVectorField.getType());

    var vectorFieldWithSpecifications = getVectorFieldDefinition("b", derivedIndexDefinition);
    Assert.assertEquals(
        VectorIndexFieldDefinition.Type.VECTOR, vectorFieldWithSpecifications.getType());
    Assert.assertEquals(
        autoEmbedFieldWithSpecifications.specification(),
        vectorFieldWithSpecifications.asVectorField().specification());

    var derivedFilterField = getVectorFieldDefinition("color", derivedIndexDefinition);
    Assert.assertEquals(VectorIndexFieldDefinition.Type.FILTER, derivedFilterField.getType());

    Assert.assertEquals(MV_DATABASE_NAME, derivedIndexDefinition.getDatabase());
    Assert.assertEquals(
        autoEmbedIndexDefinition.getIndexId().toHexString(),
        derivedIndexDefinition.getLastObservedCollectionName());
    Assert.assertEquals(Optional.empty(), derivedIndexDefinition.getView());
  }

  @Test
  public void testGetDerivedVectorIndexDefinition_version1() {

    var schemaMetadata =
        new MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata(
            1,
            Map.of(
                FieldPath.parse("b.a"),
                FieldPath.parse("_autoEmbed.b.a"),
                FieldPath.parse("a"),
                FieldPath.parse("_autoEmbed.a")));
    var defaultAutoEmbedField = new VectorAutoEmbedFieldDefinition(FieldPath.parse("a"));
    var autoEmbedFieldWithSpecifications =
        new VectorAutoEmbedFieldDefinition(
            "voyage-3-large",
            "text",
            FieldPath.parse("b.a"),
            VectorSimilarity.COSINE,
            VectorQuantization.NONE);
    var filterField = new VectorIndexFilterFieldDefinition(FieldPath.parse("color"));

    List<VectorIndexFieldDefinition> fields =
        List.of(defaultAutoEmbedField, autoEmbedFieldWithSpecifications, filterField);
    var autoEmbedIndexDefinition = VectorIndexDefinitionBuilder.builder().setFields(fields).build();

    var collectionUuid = UUID.randomUUID();
    var derivedIndexDefinition =
        AutoEmbeddingIndexDefinitionUtils.getDerivedVectorIndexDefinition(
            autoEmbedIndexDefinition, MV_DATABASE_NAME, collectionUuid, schemaMetadata);

    Assert.assertEquals(collectionUuid, derivedIndexDefinition.getCollectionUuid());
    Assert.assertEquals(
        autoEmbedIndexDefinition.getFields().size(), derivedIndexDefinition.getFields().size());

    var defaultVectorField = getVectorFieldDefinition("_autoEmbed.a", derivedIndexDefinition);
    Assert.assertEquals(VectorIndexFieldDefinition.Type.VECTOR, defaultVectorField.getType());
    Assert.assertEquals(FieldPath.parse("_autoEmbed.a"), defaultVectorField.getPath());

    var vectorFieldWithSpecifications =
        getVectorFieldDefinition("_autoEmbed.b.a", derivedIndexDefinition);
    Assert.assertEquals(
        VectorIndexFieldDefinition.Type.VECTOR, vectorFieldWithSpecifications.getType());
    Assert.assertEquals(FieldPath.parse("_autoEmbed.b.a"), vectorFieldWithSpecifications.getPath());
    Assert.assertEquals(
        autoEmbedFieldWithSpecifications.specification(),
        vectorFieldWithSpecifications.asVectorField().specification());

    var derivedFilterField = getVectorFieldDefinition("color", derivedIndexDefinition);
    Assert.assertEquals(VectorIndexFieldDefinition.Type.FILTER, derivedFilterField.getType());

    Assert.assertEquals(MV_DATABASE_NAME, derivedIndexDefinition.getDatabase());
    Assert.assertEquals(
        autoEmbedIndexDefinition.getIndexId().toHexString(),
        derivedIndexDefinition.getLastObservedCollectionName());
    Assert.assertEquals(Optional.empty(), derivedIndexDefinition.getView());
  }

  private VectorIndexFieldDefinition getVectorFieldDefinition(
      String path, VectorIndexDefinition indexDefinition) {
    return indexDefinition.getFields().stream()
        .filter(field -> field.getPath().equals(FieldPath.parse(path)))
        .findFirst()
        .get();
  }

  @Test
  public void testModalityValidation() throws Exception {
    // Test valid modality values (case-insensitive)
    List<String> validModalityValues = List.of("text", "TEXT", "Text", "tExT");
    for (String modalityValue : validModalityValues) {
      var bsonDoc =
          new BsonDocument()
              .append("path", new BsonString("field"))
              .append("model", new BsonString("voyage-3-large"))
              .append("modality", new BsonString(modalityValue));

      try (var parser = BsonDocumentParser.fromRoot(bsonDoc).build()) {
        var field = VectorAutoEmbedFieldDefinition.fromBson(parser);
        Assert.assertEquals("text", field.specification().modality());
      }
    }

    // Test invalid modality values
    List<String> invalidModalityValues = List.of("image", "audio", "video", "", "multimodal");
    for (String modalityValue : invalidModalityValues) {
      var bsonDoc =
          new BsonDocument()
              .append("path", new BsonString("field"))
              .append("model", new BsonString("voyage-3-large"))
              .append("modality", new BsonString(modalityValue));

      try (var parser = BsonDocumentParser.fromRoot(bsonDoc).build()) {
        var exception =
            Assert.assertThrows(
                BsonParseException.class, () -> VectorAutoEmbedFieldDefinition.fromBson(parser));
        Assert.assertTrue(
            "Expected error message to contain \"must be 'text'\" for modality value: "
                + modalityValue,
            exception.getMessage().contains("must be 'text'"));
      }
    }
  }

  @Test
  public void testGetMatViewIndexFields_version0() {
    var defaultAutoEmbedField = new VectorAutoEmbedFieldDefinition(FieldPath.parse("a"));
    var autoEmbedFieldWithSpecifications =
        new VectorAutoEmbedFieldDefinition(
            "voyage-3-large",
            "text",
            FieldPath.parse("b"),
            VectorSimilarity.COSINE,
            VectorQuantization.NONE);
    var filterField = new VectorIndexFilterFieldDefinition(FieldPath.parse("color"));

    List<VectorIndexFieldDefinition> fields =
        List.of(defaultAutoEmbedField, autoEmbedFieldWithSpecifications, filterField);
    var autoEmbedIndexDefinition = VectorIndexDefinitionBuilder.builder().setFields(fields).build();

    var matViewFieldMapping =
        AutoEmbeddingIndexDefinitionUtils.getMatViewIndexFields(
            autoEmbedIndexDefinition.getMappings(), MAT_VIEW_SCHEMA_METADATA);

    // 2 auto-embed fields, so 2 additional hash field entries.
    Assert.assertEquals(
        autoEmbedIndexDefinition.getFields().size() + 2, matViewFieldMapping.fieldMap().size());
    Assert.assertTrue(
        matViewFieldMapping.fieldMap().containsKey(FieldPath.parse("a" + HASH_FIELD_SUFFIX)));
    Assert.assertTrue(
        matViewFieldMapping.fieldMap().containsKey(FieldPath.parse("b" + HASH_FIELD_SUFFIX)));
  }

  @Test
  public void testGetMatViewIndexFields_version1() {
    var schemaMetadata =
        new MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata(
            1,
            Map.of(
                FieldPath.parse("b.a"),
                FieldPath.parse("_autoEmbed.b.a"),
                FieldPath.parse("a"),
                FieldPath.parse("_autoEmbed.a")));
    var defaultAutoEmbedField = new VectorAutoEmbedFieldDefinition(FieldPath.parse("a"));
    var autoEmbedFieldWithSpecifications =
        new VectorAutoEmbedFieldDefinition(
            "voyage-3-large",
            "text",
            FieldPath.parse("b.a"),
            VectorSimilarity.COSINE,
            VectorQuantization.NONE);
    var filterField = new VectorIndexFilterFieldDefinition(FieldPath.parse("color"));

    List<VectorIndexFieldDefinition> fields =
        List.of(defaultAutoEmbedField, autoEmbedFieldWithSpecifications, filterField);
    var autoEmbedIndexDefinition = VectorIndexDefinitionBuilder.builder().setFields(fields).build();

    var matViewFieldMapping =
        AutoEmbeddingIndexDefinitionUtils.getMatViewIndexFields(
            autoEmbedIndexDefinition.getMappings(), schemaMetadata);

    // 2 auto-embed fields, so 2 additional hash field entries.
    Assert.assertEquals(
        autoEmbedIndexDefinition.getFields().size() + 2, matViewFieldMapping.fieldMap().size());
    Assert.assertTrue(
        matViewFieldMapping
            .fieldMap()
            .containsKey(FieldPath.parse("_autoEmbed." + HASH_FIELD_SUFFIX + ".b.a")));
    Assert.assertTrue(
        matViewFieldMapping
            .fieldMap()
            .containsKey(FieldPath.parse("_autoEmbed." + HASH_FIELD_SUFFIX + ".a")));
  }

  @Test
  public void testGetHashFieldPathRootLevel() {
    Assert.assertEquals(
        "a" + HASH_FIELD_SUFFIX,
        AutoEmbeddingIndexDefinitionUtils.getHashFieldPath(FieldPath.parse("a"), 0).toString());
  }

  @Test
  public void testGetHashFieldPathNested() {
    Assert.assertEquals(
        "a.b.c" + HASH_FIELD_SUFFIX,
        AutoEmbeddingIndexDefinitionUtils.getHashFieldPath(FieldPath.parse("a.b.c"), 0).toString());
  }
}
