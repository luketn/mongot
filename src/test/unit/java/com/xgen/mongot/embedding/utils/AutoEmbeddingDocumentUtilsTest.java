package com.xgen.mongot.embedding.utils;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.mongot.embedding.utils.AutoEmbeddingDocumentUtils.buildAutoEmbeddingDocumentEvent;
import static com.xgen.mongot.embedding.utils.AutoEmbeddingDocumentUtils.buildMaterializedViewDocumentEvent;
import static com.xgen.mongot.embedding.utils.AutoEmbeddingDocumentUtils.compareDocuments;
import static com.xgen.mongot.embedding.utils.AutoEmbeddingDocumentUtils.getVectorTextPathMap;
import static com.xgen.mongot.embedding.utils.ReplaceStringsFieldValueHandler.HASH_FIELD_SUFFIX;
import static com.xgen.mongot.embedding.utils.ReplaceStringsFieldValueHandler.computeTextHash;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;
import com.google.common.hash.Hashing;
import com.google.errorprone.annotations.Var;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata;
import com.xgen.mongot.index.DocumentEvent;
import com.xgen.mongot.index.DocumentMetadata;
import com.xgen.mongot.index.definition.VectorAutoEmbedFieldDefinition;
import com.xgen.mongot.index.definition.VectorIndexFieldDefinition;
import com.xgen.mongot.index.definition.VectorIndexFieldMapping;
import com.xgen.mongot.index.definition.VectorIndexFilterFieldDefinition;
import com.xgen.mongot.index.definition.VectorTextFieldDefinition;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.BsonVectorParser;
import com.xgen.mongot.util.bson.FloatVector;
import com.xgen.mongot.util.bson.Vector;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.RawBsonDocument;
import org.junit.Test;

public class AutoEmbeddingDocumentUtilsTest {

  private static final MaterializedViewSchemaMetadata MAT_VIEW_SCHEMA_METADATA =
      new MaterializedViewSchemaMetadata(0, Map.of());

  @Test
  public void testGetTextValues() throws IOException {
    BsonDocument bsonDoc = createBasicBson();
    RawBsonDocument rawBsonDoc = new RawBsonDocument(bsonDoc, BsonUtils.BSON_DOCUMENT_CODEC);
    List<VectorIndexFieldDefinition> fields =
        List.of(
            new VectorTextFieldDefinition(FieldPath.parse("a")),
            new VectorTextFieldDefinition(FieldPath.parse("b")));
    VectorIndexFieldMapping mappings = VectorIndexFieldMapping.create(fields);

    Map<FieldPath, Set<String>> result = getVectorTextPathMap(rawBsonDoc, mappings);
    assertEquals(
        ImmutableMap.of(
            FieldPath.parse("a"), Set.of("aString"), FieldPath.parse("b"), Set.of("bString")),
        result);
  }

  @Test
  public void testGetEmbeddedTextValues() throws IOException {
    List<VectorIndexFieldDefinition> fields =
        List.of(
            new VectorTextFieldDefinition(FieldPath.parse("root.a")),
            new VectorTextFieldDefinition(FieldPath.parse("root.b")),
            new VectorTextFieldDefinition(FieldPath.newRoot("dot.field")));
    VectorIndexFieldMapping mappings = VectorIndexFieldMapping.create(fields);
    BsonDocument bsonDoc = createEmbeddedBson();

    // also test that "." in field name works
    bsonDoc.append("dot.field", new BsonString("dotString"));
    RawBsonDocument rawBsonDoc = new RawBsonDocument(bsonDoc, BsonUtils.BSON_DOCUMENT_CODEC);

    Map<FieldPath, Set<String>> result = getVectorTextPathMap(rawBsonDoc, mappings);
    assertEquals(
        ImmutableMap.of(
            FieldPath.parse("root.a"),
            Set.of("aString"),
            FieldPath.parse("root.b"),
            Set.of("bString"),
            FieldPath.newRoot("dot.field"),
            Set.of("dotString")),
        result);
  }

  @Test
  public void testGetArrayTextValues() throws IOException {
    List<VectorIndexFieldDefinition> fields =
        List.of(
            new VectorTextFieldDefinition(FieldPath.parse("root")),
            new VectorTextFieldDefinition(FieldPath.parse("root.a")));
    VectorIndexFieldMapping mappings = VectorIndexFieldMapping.create(fields);
    BsonDocument bsonDoc = createArrayBson();
    RawBsonDocument rawBsonDoc = new RawBsonDocument(bsonDoc, BsonUtils.BSON_DOCUMENT_CODEC);

    Map<FieldPath, Set<String>> result = getVectorTextPathMap(rawBsonDoc, mappings);
    assertEquals(
        ImmutableMap.of(
            FieldPath.parse("root"),
            Set.of("arrayString1", "arrayString2"),
            FieldPath.parse("root.a"),
            Set.of("aString", "aString2")),
        result);
  }

  @Test
  public void testGetIgnoredTextValues() throws IOException {
    List<VectorIndexFieldDefinition> fields =
        List.of(
            new VectorTextFieldDefinition(FieldPath.parse("root")),
            new VectorTextFieldDefinition(FieldPath.parse("root.b")),
            new VectorTextFieldDefinition(FieldPath.parse("d")),
            new VectorTextFieldDefinition(FieldPath.parse("a")),
            new VectorTextFieldDefinition(FieldPath.parse("num")));
    VectorIndexFieldMapping mappings = VectorIndexFieldMapping.create(fields);
    BsonDocument bsonDoc = createArrayBson();
    bsonDoc.append("d", new BsonString("dString"));
    bsonDoc.append("num", new BsonDouble(1.23));
    RawBsonDocument rawBsonDoc = new RawBsonDocument(bsonDoc, BsonUtils.BSON_DOCUMENT_CODEC);

    Map<FieldPath, Set<String>> result = getVectorTextPathMap(rawBsonDoc, mappings);
    assertEquals(
        Map.of(
            FieldPath.parse("d"),
            Set.of("dString"),
            FieldPath.parse("root.b"),
            Set.of("bString"),
            FieldPath.parse("root"),
            Set.of("arrayString1", "arrayString2")),
        result);
  }

  @Test
  public void testBuildDocumentEvent() throws IOException {
    List<VectorIndexFieldDefinition> fields =
        List.of(
            new VectorTextFieldDefinition(FieldPath.parse("a")),
            new VectorTextFieldDefinition(FieldPath.parse("b")),
            new VectorTextFieldDefinition(FieldPath.parse("c")),
            new VectorTextFieldDefinition(FieldPath.parse("extra")),
            new VectorTextFieldDefinition(FieldPath.parse("num")));
    VectorIndexFieldMapping mappings = VectorIndexFieldMapping.create(fields);
    ImmutableMap<String, Vector> embeddings = createEmbeddings();
    BsonDocument bsonDoc = createBasicBson();
    bsonDoc.append("extra", new BsonString("no-embedding"));
    bsonDoc.append("num", new BsonDouble(1.23));
    RawBsonDocument rawBsonDoc = new RawBsonDocument(bsonDoc, BsonUtils.BSON_DOCUMENT_CODEC);
    DocumentEvent rawDocumentEvent =
        DocumentEvent.createInsert(
            DocumentMetadata.fromOriginalDocument(Optional.of(rawBsonDoc)), rawBsonDoc);

    DocumentEvent result =
        buildAutoEmbeddingDocumentEvent(
            rawDocumentEvent,
            mappings,
            mappings.fieldMap().keySet().stream()
                .map(fieldPath -> new AbstractMap.SimpleEntry<>(fieldPath, embeddings))
                .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue)));

    assertEquals(
        DocumentEvent.createFromDocumentEventAndVectors(
            rawDocumentEvent,
            ImmutableMap.of(
                FieldPath.parse("a"),
                ImmutableMap.of("aString", embeddings.get("aString")),
                FieldPath.parse("b"),
                ImmutableMap.of("bString", embeddings.get("bString")),
                FieldPath.parse("c"),
                ImmutableMap.of(), // no matched vector in embeddings
                FieldPath.parse("extra"),
                ImmutableMap.of() // no matched vector in embeddings
                )),
        result);
  }

  @Test
  public void testBuildDocumentEvent_noop() throws IOException {
    List<VectorIndexFieldDefinition> fields =
        List.of(new VectorTextFieldDefinition(FieldPath.parse("a")));
    VectorIndexFieldMapping mappings = VectorIndexFieldMapping.create(fields);
    ImmutableMap<String, Vector> embeddings = createEmbeddings();
    DocumentEvent rawDocumentEvent = DocumentEvent.createDelete(new BsonInt32(1));
    DocumentEvent result =
        buildAutoEmbeddingDocumentEvent(
            rawDocumentEvent, mappings, ImmutableMap.of(FieldPath.parse("a"), embeddings));
    assertEquals(rawDocumentEvent, result);
  }

  @Test
  public void testBuildDocumentEvent_embeddedDoc() throws IOException {
    List<VectorIndexFieldDefinition> fields =
        List.of(
            new VectorTextFieldDefinition(FieldPath.parse("root.a")),
            new VectorTextFieldDefinition(FieldPath.parse("root.b")),
            new VectorTextFieldDefinition(FieldPath.newRoot("dot.field")));
    VectorIndexFieldMapping mappings = VectorIndexFieldMapping.create(fields);
    ImmutableMap<String, Vector> embeddings = createEmbeddings();
    BsonDocument bsonDoc = createEmbeddedBson();

    // also test that "." in field name works
    bsonDoc.append("dot.field", new BsonString("fString"));

    RawBsonDocument rawBsonDoc = new RawBsonDocument(bsonDoc, BsonUtils.BSON_DOCUMENT_CODEC);
    DocumentEvent rawDocumentEvent =
        DocumentEvent.createInsert(
            DocumentMetadata.fromOriginalDocument(Optional.of(rawBsonDoc)), rawBsonDoc);

    DocumentEvent result =
        buildAutoEmbeddingDocumentEvent(
            rawDocumentEvent,
            mappings,
            ImmutableMap.of(
                FieldPath.parse("root.a"),
                embeddings,
                FieldPath.parse("root.b"),
                embeddings,
                FieldPath.newRoot("dot.field"),
                embeddings));

    ImmutableMap<FieldPath, ImmutableMap<String, Vector>> perDocEmbeddings =
        ImmutableMap.of(
            FieldPath.parse("root.a"),
            ImmutableMap.of("aString", embeddings.get("aString")),
            FieldPath.parse("root.b"),
            ImmutableMap.of("bString", embeddings.get("bString")),
            FieldPath.newRoot("dot.field"),
            ImmutableMap.of("fString", embeddings.get("fString")));
    assertEquals(
        DocumentEvent.createFromDocumentEventAndVectors(rawDocumentEvent, perDocEmbeddings),
        result);
  }

  @Test
  public void testBuildDocumentEvent_array() throws IOException {
    List<VectorIndexFieldDefinition> fields =
        List.of(new VectorTextFieldDefinition(FieldPath.parse("root")));
    VectorIndexFieldMapping mappings = VectorIndexFieldMapping.create(fields);
    ImmutableMap<String, Vector> embeddings = createEmbeddings();
    BsonDocument bsonDoc = createArrayBson();
    RawBsonDocument rawBsonDoc = new RawBsonDocument(bsonDoc, BsonUtils.BSON_DOCUMENT_CODEC);
    DocumentEvent rawDocumentEvent =
        DocumentEvent.createInsert(
            DocumentMetadata.fromOriginalDocument(Optional.of(rawBsonDoc)), rawBsonDoc);
    DocumentEvent result =
        buildAutoEmbeddingDocumentEvent(
            rawDocumentEvent, mappings, ImmutableMap.of(FieldPath.parse("root"), embeddings));
    assertEquals(
        DocumentEvent.createFromDocumentEventAndVectors(
            rawDocumentEvent,
            ImmutableMap.of(
                FieldPath.parse("root"),
                ImmutableMap.of(
                    "arrayString1",
                    embeddings.get("arrayString1"),
                    "arrayString2",
                    embeddings.get("arrayString2")))),
        result);
  }

  @Test
  public void testBuildDocumentEvent_arrayEmbeddedDoc() throws IOException {
    List<VectorIndexFieldDefinition> fields =
        List.of(
            new VectorTextFieldDefinition(FieldPath.parse("root")),
            new VectorTextFieldDefinition(FieldPath.parse("root.a")),
            new VectorTextFieldDefinition(FieldPath.parse("root.b")));
    VectorIndexFieldMapping mappings = VectorIndexFieldMapping.create(fields);
    ImmutableMap<String, Vector> embeddings = createEmbeddings();
    BsonDocument bsonDoc = createArrayBson();
    RawBsonDocument rawBsonDoc = new RawBsonDocument(bsonDoc, BsonUtils.BSON_DOCUMENT_CODEC);
    DocumentEvent rawDocumentEvent =
        DocumentEvent.createInsert(
            DocumentMetadata.fromOriginalDocument(Optional.of(rawBsonDoc)), rawBsonDoc);

    ImmutableMap<FieldPath, ImmutableMap<String, Vector>> allEmbeddingsFromBatchResponse =
        ImmutableMap.of(
            FieldPath.parse("root.b"), embeddings, FieldPath.parse("root.a"), embeddings);
    DocumentEvent result =
        buildAutoEmbeddingDocumentEvent(rawDocumentEvent, mappings, allEmbeddingsFromBatchResponse);
    assertEquals(
        DocumentEvent.createFromDocumentEventAndVectors(
            rawDocumentEvent,
            // Expects no entry for FieldPath.parse("root"), since allEmbeddingsFromBatchResponse
            // has no vector for it.
            ImmutableMap.of(
                // Expects no aString entry in FieldPath.parse("root.a"), since no results in
                // allEmbeddingsFromBatchResponse
                FieldPath.parse("root.a"), ImmutableMap.of("aString", embeddings.get("aString")),
                FieldPath.parse("root.b"), ImmutableMap.of("bString", embeddings.get("bString")))),
        result);
  }

  @Test
  public void testBuildMaterializedViewDocumentEvent() throws IOException {
    List<VectorIndexFieldDefinition> fields =
        List.of(
            new VectorAutoEmbedFieldDefinition(FieldPath.parse("a")),
            new VectorAutoEmbedFieldDefinition(FieldPath.parse("b")),
            new VectorAutoEmbedFieldDefinition(FieldPath.parse("c")),
            new VectorAutoEmbedFieldDefinition(FieldPath.parse("extra")),
            new VectorAutoEmbedFieldDefinition(FieldPath.parse("num")),
            new VectorIndexFilterFieldDefinition(FieldPath.parse("color")));
    VectorIndexFieldMapping mappings = VectorIndexFieldMapping.create(fields);
    ImmutableMap<String, Vector> embeddings = createEmbeddings();
    BsonDocument bsonDoc = createBasicBson();
    bsonDoc.append("extra", new BsonString("no-embedding"));
    bsonDoc.append("num", new BsonDouble(1.23));
    bsonDoc.append("color", new BsonString("red"));
    RawBsonDocument rawBsonDoc = new RawBsonDocument(bsonDoc, BsonUtils.BSON_DOCUMENT_CODEC);
    DocumentEvent rawDocumentEvent =
        DocumentEvent.createInsert(
            DocumentMetadata.fromOriginalDocument(Optional.of(rawBsonDoc)), rawBsonDoc);

    DocumentEvent result =
        buildMaterializedViewDocumentEvent(
            rawDocumentEvent, mappings, createEmbeddingsPerField(mappings, embeddings));

    assertEquals(
        createBasicMaterializedViewBson(bsonDoc, embeddings, mappings), result.getDocument().get());

    assertEquals(rawDocumentEvent.getDocumentId(), result.getDocumentId());
  }

  @Test
  public void testNeedsReIndexing_DocumentsMatch_version0() throws IOException {
    List<VectorIndexFieldDefinition> fields =
        List.of(
            new VectorAutoEmbedFieldDefinition(FieldPath.parse("a")),
            new VectorAutoEmbedFieldDefinition(FieldPath.parse("b")),
            new VectorIndexFilterFieldDefinition(FieldPath.parse("color")));
    VectorIndexFieldMapping mappings = VectorIndexFieldMapping.create(fields);
    ImmutableMap<String, Vector> embeddings = createEmbeddings();
    BsonDocument bsonDoc = createBasicBson();
    RawBsonDocument rawBsonDoc = new RawBsonDocument(bsonDoc, BsonUtils.BSON_DOCUMENT_CODEC);
    DocumentEvent rawDocumentEvent =
        DocumentEvent.createInsert(
            DocumentMetadata.fromOriginalDocument(Optional.of(rawBsonDoc)), rawBsonDoc);

    DocumentEvent result =
        buildMaterializedViewDocumentEvent(
            rawDocumentEvent, mappings, createEmbeddingsPerField(mappings, embeddings));

    var comparisonResult =
        compareDocuments(
            rawDocumentEvent.getDocument().get(),
            result.getDocument().get(),
            mappings,
            AutoEmbeddingIndexDefinitionUtils.getMatViewIndexFields(
                mappings, MAT_VIEW_SCHEMA_METADATA),
            MAT_VIEW_SCHEMA_METADATA);

    assertFalse(comparisonResult.needsReIndexing());
    assertEquals(2, comparisonResult.reusableEmbeddings().size());
    assertTrue(comparisonResult.reusableEmbeddings().containsKey(FieldPath.parse("a")));
    assertTrue(comparisonResult.reusableEmbeddings().containsKey(FieldPath.parse("b")));
  }

  @Test
  public void testNeedsReIndexing_DocumentsMatch_version1() throws IOException {
    List<VectorIndexFieldDefinition> fields =
        List.of(
            new VectorAutoEmbedFieldDefinition(FieldPath.parse("a")),
            new VectorAutoEmbedFieldDefinition(FieldPath.parse("b")),
            new VectorIndexFilterFieldDefinition(FieldPath.parse("color")));
    var schemaMetadata =
        new MaterializedViewSchemaMetadata(
            1,
            Map.of(
                FieldPath.parse("a"),
                FieldPath.parse("_autoEmbed.a"),
                FieldPath.parse("b"),
                FieldPath.parse("_autoEmbed.b")));
    VectorIndexFieldMapping mappings = VectorIndexFieldMapping.create(fields);
    VectorIndexFieldMapping matViewMappingsWithHash =
        AutoEmbeddingIndexDefinitionUtils.getMatViewIndexFields(mappings, schemaMetadata);
    ImmutableMap<String, Vector> embeddings = createEmbeddings();
    // TODO(CLOUDP-363914): Refactor this once we integrate schema metadata in
    // AutoEmbeddingDocumentUtils::buildMaterializedViewDocumentEvent, for now, we needs to manually
    // add hash field.
    BsonDocument bsonDoc = new BsonDocument();
    bsonDoc
        .append("_id", new BsonString("anId"))
        .append("_autoEmbed.a", new BsonString("aString"))
        .append("_autoEmbed._hash.a", new BsonString(computeTextHash("aString")))
        .append("_autoEmbed.b", new BsonString("bString"))
        .append("_autoEmbed._hash.b", new BsonString(computeTextHash("bString")))
        .append("_autoEmbed.c", new BsonString("cString"))
        .append("_autoEmbed._hash.c", new BsonString(computeTextHash("cString")));
    RawBsonDocument rawBsonDoc = new RawBsonDocument(bsonDoc, BsonUtils.BSON_DOCUMENT_CODEC);
    DocumentEvent rawDocumentEvent =
        DocumentEvent.createInsert(
            DocumentMetadata.fromOriginalDocument(Optional.of(rawBsonDoc)), rawBsonDoc);
    DocumentEvent result =
        buildMaterializedViewDocumentEvent(
            rawDocumentEvent,
            matViewMappingsWithHash,
            createEmbeddingsPerField(matViewMappingsWithHash, embeddings));
    var comparisonResult =
        compareDocuments(
            new RawBsonDocument(createBasicBson(), BsonUtils.BSON_DOCUMENT_CODEC),
            result.getDocument().get(),
            mappings,
            matViewMappingsWithHash,
            schemaMetadata);

    assertTrue(comparisonResult.needsReIndexing());
    assertEquals(2, comparisonResult.reusableEmbeddings().size());
    assertTrue(comparisonResult.reusableEmbeddings().containsKey(FieldPath.parse("a")));
    assertTrue(comparisonResult.reusableEmbeddings().containsKey(FieldPath.parse("b")));
  }

  @Test
  public void testCompareDocumentsEmptyStringField() throws IOException {
    List<VectorIndexFieldDefinition> fields =
        List.of(new VectorAutoEmbedFieldDefinition(FieldPath.parse("c")));
    VectorIndexFieldMapping mappings = VectorIndexFieldMapping.create(fields);
    ImmutableMap<String, Vector> embeddings = createEmbeddings();
    BsonDocument bsonDoc = createBsonWithGivenString("");
    RawBsonDocument rawBsonDoc = new RawBsonDocument(bsonDoc, BsonUtils.BSON_DOCUMENT_CODEC);
    DocumentEvent rawDocumentEvent =
        DocumentEvent.createInsert(
            DocumentMetadata.fromOriginalDocument(Optional.of(rawBsonDoc)), rawBsonDoc);

    DocumentEvent result =
        buildMaterializedViewDocumentEvent(
            rawDocumentEvent, mappings, createEmbeddingsPerField(mappings, embeddings));

    var comparisonResult =
        compareDocuments(
            rawDocumentEvent.getDocument().get(),
            result.getDocument().get(),
            mappings,
            AutoEmbeddingIndexDefinitionUtils.getMatViewIndexFields(
                mappings, MAT_VIEW_SCHEMA_METADATA),
            MAT_VIEW_SCHEMA_METADATA);

    assertFalse(comparisonResult.needsReIndexing());
    assertEquals(0, comparisonResult.reusableEmbeddings().size());
  }

  @Test
  public void testCompareDocumentsNonEmptyToEmptyStringField() throws IOException {
    List<VectorIndexFieldDefinition> fields =
        List.of(new VectorAutoEmbedFieldDefinition(FieldPath.parse("c")));
    VectorIndexFieldMapping mappings = VectorIndexFieldMapping.create(fields);
    ImmutableMap<String, Vector> embeddings = createEmbeddings();
    @Var BsonDocument bsonDoc = createBsonWithGivenString("aString");
    @Var RawBsonDocument rawBsonDoc = new RawBsonDocument(bsonDoc, BsonUtils.BSON_DOCUMENT_CODEC);
    @Var
    DocumentEvent rawDocumentEvent =
        DocumentEvent.createInsert(
            DocumentMetadata.fromOriginalDocument(Optional.of(rawBsonDoc)), rawBsonDoc);

    DocumentEvent result =
        buildMaterializedViewDocumentEvent(
            rawDocumentEvent, mappings, createEmbeddingsPerField(mappings, embeddings));

    @Var
    var comparisonResult =
        compareDocuments(
            rawDocumentEvent.getDocument().get(),
            result.getDocument().get(),
            mappings,
            AutoEmbeddingIndexDefinitionUtils.getMatViewIndexFields(
                mappings, MAT_VIEW_SCHEMA_METADATA),
            MAT_VIEW_SCHEMA_METADATA);

    assertFalse(comparisonResult.needsReIndexing());
    assertEquals(1, comparisonResult.reusableEmbeddings().size());

    // Update the source collection doc to have an empty string.
    bsonDoc = createBsonWithGivenString("");
    rawBsonDoc = new RawBsonDocument(bsonDoc, BsonUtils.BSON_DOCUMENT_CODEC);
    rawDocumentEvent =
        DocumentEvent.createInsert(
            DocumentMetadata.fromOriginalDocument(Optional.of(rawBsonDoc)), rawBsonDoc);

    comparisonResult =
        compareDocuments(
            rawDocumentEvent.getDocument().get(),
            result.getDocument().get(),
            mappings,
            AutoEmbeddingIndexDefinitionUtils.getMatViewIndexFields(
                mappings, MAT_VIEW_SCHEMA_METADATA),
            MAT_VIEW_SCHEMA_METADATA);

    assertTrue(comparisonResult.needsReIndexing());
    assertEquals(0, comparisonResult.reusableEmbeddings().size());
  }

  @Test
  public void testCompareDocumentsEmptyStringFieldInArray() throws IOException {
    List<VectorIndexFieldDefinition> fields =
        List.of(new VectorAutoEmbedFieldDefinition(FieldPath.parse("root.a")));
    VectorIndexFieldMapping mappings = VectorIndexFieldMapping.create(fields);
    ImmutableMap<String, Vector> embeddings = createEmbeddings();
    BsonDocument bsonDoc = createArrayBsonWithGivenString("");
    RawBsonDocument rawBsonDoc = new RawBsonDocument(bsonDoc, BsonUtils.BSON_DOCUMENT_CODEC);
    DocumentEvent rawDocumentEvent =
        DocumentEvent.createInsert(
            DocumentMetadata.fromOriginalDocument(Optional.of(rawBsonDoc)), rawBsonDoc);

    DocumentEvent result =
        buildMaterializedViewDocumentEvent(
            rawDocumentEvent, mappings, createEmbeddingsPerField(mappings, embeddings));

    var comparisonResult =
        compareDocuments(
            rawDocumentEvent.getDocument().get(),
            result.getDocument().get(),
            mappings,
            AutoEmbeddingIndexDefinitionUtils.getMatViewIndexFields(
                mappings, MAT_VIEW_SCHEMA_METADATA),
            MAT_VIEW_SCHEMA_METADATA);

    assertFalse(comparisonResult.needsReIndexing());
    assertEquals(1, comparisonResult.reusableEmbeddings().size());
  }

  @Test
  public void testCompareDocumentsNonEmptyToEmptyStringFieldInArray() throws IOException {
    List<VectorIndexFieldDefinition> fields =
        List.of(new VectorAutoEmbedFieldDefinition(FieldPath.parse("root.a")));
    VectorIndexFieldMapping mappings = VectorIndexFieldMapping.create(fields);
    ImmutableMap<String, Vector> embeddings = createEmbeddings();
    @Var BsonDocument bsonDoc = createArrayBsonWithGivenString("aString");
    @Var RawBsonDocument rawBsonDoc = new RawBsonDocument(bsonDoc, BsonUtils.BSON_DOCUMENT_CODEC);
    @Var
    DocumentEvent rawDocumentEvent =
        DocumentEvent.createInsert(
            DocumentMetadata.fromOriginalDocument(Optional.of(rawBsonDoc)), rawBsonDoc);

    DocumentEvent result =
        buildMaterializedViewDocumentEvent(
            rawDocumentEvent, mappings, createEmbeddingsPerField(mappings, embeddings));

    // Update one of the array fields to an empty string.
    bsonDoc = createArrayBsonWithGivenString("");
    rawBsonDoc = new RawBsonDocument(bsonDoc, BsonUtils.BSON_DOCUMENT_CODEC);
    rawDocumentEvent =
        DocumentEvent.createInsert(
            DocumentMetadata.fromOriginalDocument(Optional.of(rawBsonDoc)), rawBsonDoc);

    var comparisonResult =
        compareDocuments(
            rawDocumentEvent.getDocument().get(),
            result.getDocument().get(),
            mappings,
            AutoEmbeddingIndexDefinitionUtils.getMatViewIndexFields(
                mappings, MAT_VIEW_SCHEMA_METADATA),
            MAT_VIEW_SCHEMA_METADATA);

    assertTrue(comparisonResult.needsReIndexing());
    assertEquals(1, comparisonResult.reusableEmbeddings().size());
  }

  @Test
  public void testNeedsReIndexing_EmbeddingsMismatch() throws IOException {
    List<VectorIndexFieldDefinition> fields =
        List.of(
            new VectorAutoEmbedFieldDefinition(FieldPath.parse("a")),
            new VectorAutoEmbedFieldDefinition(FieldPath.parse("b")),
            new VectorAutoEmbedFieldDefinition(FieldPath.parse("c")),
            new VectorAutoEmbedFieldDefinition(FieldPath.parse("extra")),
            new VectorAutoEmbedFieldDefinition(FieldPath.parse("num")),
            new VectorIndexFilterFieldDefinition(FieldPath.parse("color")));
    VectorIndexFieldMapping mappings = VectorIndexFieldMapping.create(fields);
    ImmutableMap<String, Vector> embeddings = createEmbeddings();
    BsonDocument bsonDoc = createBasicBson();
    bsonDoc.append("extra", new BsonString("no-embedding"));
    bsonDoc.append("num", new BsonDouble(1.23));
    bsonDoc.append("color", new BsonString("red"));
    RawBsonDocument rawBsonDoc = new RawBsonDocument(bsonDoc, BsonUtils.BSON_DOCUMENT_CODEC);
    DocumentEvent rawDocumentEvent =
        DocumentEvent.createInsert(
            DocumentMetadata.fromOriginalDocument(Optional.of(rawBsonDoc)), rawBsonDoc);

    DocumentEvent result =
        buildMaterializedViewDocumentEvent(
            rawDocumentEvent, mappings, createEmbeddingsPerField(mappings, embeddings));

    var comparisonResult =
        compareDocuments(
            rawDocumentEvent.getDocument().get(),
            result.getDocument().get(),
            mappings,
            AutoEmbeddingIndexDefinitionUtils.getMatViewIndexFields(
                mappings, MAT_VIEW_SCHEMA_METADATA),
            MAT_VIEW_SCHEMA_METADATA);

    // Expect re-indexing since only 2 of the 3 auto-embedding fields have embeddings in the mat
    // view.
    assertTrue(comparisonResult.needsReIndexing());
    assertEquals(2, comparisonResult.reusableEmbeddings().size());
    assertTrue(comparisonResult.reusableEmbeddings().containsKey(FieldPath.parse("a")));
    assertTrue(comparisonResult.reusableEmbeddings().containsKey(FieldPath.parse("b")));
  }

  @Test
  public void testNeedsReIndexing_FilterAddition() throws IOException {
    List<VectorIndexFieldDefinition> fields =
        List.of(
            new VectorAutoEmbedFieldDefinition(FieldPath.parse("a")),
            new VectorAutoEmbedFieldDefinition(FieldPath.parse("b")),
            new VectorIndexFilterFieldDefinition(FieldPath.parse("color")));
    @Var VectorIndexFieldMapping mappings = VectorIndexFieldMapping.create(fields);
    ImmutableMap<String, Vector> embeddings = createEmbeddings();
    BsonDocument bsonDoc = createBasicBson();
    bsonDoc.append("color", new BsonString("red"));
    @Var RawBsonDocument rawBsonDoc = new RawBsonDocument(bsonDoc, BsonUtils.BSON_DOCUMENT_CODEC);
    @Var
    DocumentEvent rawDocumentEvent =
        DocumentEvent.createInsert(
            DocumentMetadata.fromOriginalDocument(Optional.of(rawBsonDoc)), rawBsonDoc);

    DocumentEvent result =
        buildMaterializedViewDocumentEvent(
            rawDocumentEvent, mappings, createEmbeddingsPerField(mappings, embeddings));

    // add a new filter field and update the source collection doc.
    var newFields =
        Streams.concat(
                fields.stream(),
                Stream.of(new VectorIndexFilterFieldDefinition(FieldPath.parse("size"))))
            .toList();
    bsonDoc.append("size", new BsonString("large"));
    rawBsonDoc = new RawBsonDocument(bsonDoc, BsonUtils.BSON_DOCUMENT_CODEC);
    rawDocumentEvent =
        DocumentEvent.createInsert(
            DocumentMetadata.fromOriginalDocument(Optional.of(rawBsonDoc)), rawBsonDoc);
    mappings = VectorIndexFieldMapping.create(newFields);

    var comparisonResult =
        compareDocuments(
            rawDocumentEvent.getDocument().get(),
            result.getDocument().get(),
            mappings,
            AutoEmbeddingIndexDefinitionUtils.getMatViewIndexFields(
                mappings, MAT_VIEW_SCHEMA_METADATA),
            MAT_VIEW_SCHEMA_METADATA);

    // Expect re-indexing since filter fields dont match
    assertTrue(comparisonResult.needsReIndexing());
    assertEquals(2, comparisonResult.reusableEmbeddings().size());
    assertTrue(comparisonResult.reusableEmbeddings().containsKey(FieldPath.parse("a")));
    assertTrue(comparisonResult.reusableEmbeddings().containsKey(FieldPath.parse("b")));
  }

  @Test
  public void testNeedsReIndexing_FilterRemoval() throws IOException {
    List<VectorIndexFieldDefinition> fields =
        List.of(
            new VectorAutoEmbedFieldDefinition(FieldPath.parse("a")),
            new VectorAutoEmbedFieldDefinition(FieldPath.parse("b")),
            new VectorIndexFilterFieldDefinition(FieldPath.parse("color")),
            new VectorIndexFilterFieldDefinition(FieldPath.parse("size")));
    @Var VectorIndexFieldMapping mappings = VectorIndexFieldMapping.create(fields);
    ImmutableMap<String, Vector> embeddings = createEmbeddings();
    BsonDocument bsonDoc = createBasicBson();
    bsonDoc.append("color", new BsonString("red"));
    bsonDoc.append("size", new BsonString("large"));
    RawBsonDocument rawBsonDoc = new RawBsonDocument(bsonDoc, BsonUtils.BSON_DOCUMENT_CODEC);
    DocumentEvent rawDocumentEvent =
        DocumentEvent.createInsert(
            DocumentMetadata.fromOriginalDocument(Optional.of(rawBsonDoc)), rawBsonDoc);

    DocumentEvent result =
        buildMaterializedViewDocumentEvent(
            rawDocumentEvent, mappings, createEmbeddingsPerField(mappings, embeddings));

    // remove a filter field and update the source collection doc.
    var newFields =
        List.of(
            new VectorAutoEmbedFieldDefinition(FieldPath.parse("a")),
            new VectorAutoEmbedFieldDefinition(FieldPath.parse("b")),
            new VectorIndexFilterFieldDefinition(FieldPath.parse("color")));
    mappings = VectorIndexFieldMapping.create(newFields);

    var comparisonResult =
        compareDocuments(
            rawDocumentEvent.getDocument().get(),
            result.getDocument().get(),
            mappings,
            AutoEmbeddingIndexDefinitionUtils.getMatViewIndexFields(
                mappings, MAT_VIEW_SCHEMA_METADATA),
            MAT_VIEW_SCHEMA_METADATA);

    // Expect re-indexing since filter fields dont match
    assertTrue(comparisonResult.needsReIndexing());
    assertEquals(2, comparisonResult.reusableEmbeddings().size());
    assertTrue(comparisonResult.reusableEmbeddings().containsKey(FieldPath.parse("a")));
    assertTrue(comparisonResult.reusableEmbeddings().containsKey(FieldPath.parse("b")));
  }

  private BsonDocument createBasicBson() {
    BsonDocument bsonDoc = new BsonDocument();
    bsonDoc
        .append("_id", new BsonString("anId"))
        .append("a", new BsonString("aString"))
        .append("b", new BsonString("bString"))
        .append("c", new BsonString("cString"));
    return bsonDoc;
  }

  private BsonDocument createBsonWithGivenString(String value) {
    BsonDocument bsonDoc = new BsonDocument();
    bsonDoc.append("_id", new BsonString("anId")).append("c", new BsonString(value));
    return bsonDoc;
  }

  private BsonDocument createBasicMaterializedViewBson(
      BsonDocument originalDoc, Map<String, Vector> embeddings, VectorIndexFieldMapping mappings) {
    BsonDocument bsonDoc = new BsonDocument();

    // Iterate over the mapping's field paths
    for (Map.Entry<FieldPath, VectorIndexFieldDefinition> entry : mappings.fieldMap().entrySet()) {
      FieldPath fieldPath = entry.getKey();
      VectorIndexFieldDefinition fieldDefinition = entry.getValue();

      // Only process fields that are in the original document
      String fieldName = fieldPath.toString();
      if (!originalDoc.containsKey(fieldName)) {
        continue;
      }

      if (fieldDefinition.getType() == VectorIndexFieldDefinition.Type.AUTO_EMBED) {
        // For VectorTextFieldDefinition, replace text with vector
        if (originalDoc.get(fieldName).isString()) {
          String textValue = originalDoc.get(fieldName).asString().getValue();
          if (embeddings.containsKey(textValue)) {
            bsonDoc.append(fieldName, BsonVectorParser.encode(embeddings.get(textValue)));
            // Add hash field for the text value (only when embedding exists)
            String hash = Hashing.sha256().hashString(textValue, StandardCharsets.UTF_8).toString();
            bsonDoc.append(fieldName + HASH_FIELD_SUFFIX, new BsonString(hash));
          }
        }
      } else if (fieldDefinition.getType() == VectorIndexFieldDefinition.Type.FILTER) {
        // For VectorFilterFieldDefinition, copy as-is
        bsonDoc.append(fieldName, originalDoc.get(fieldName));
      }
    }
    return bsonDoc;
  }

  private ImmutableMap<String, Vector> createEmbeddings() {
    Vector vector1 = Vector.fromFloats(new float[] {1.0f, 2.0f}, FloatVector.OriginalType.NATIVE);
    Vector vector2 = Vector.fromFloats(new float[] {5.0f, 6.0f}, FloatVector.OriginalType.NATIVE);
    Vector vector3 = Vector.fromFloats(new float[] {9.0f, 10.0f}, FloatVector.OriginalType.NATIVE);
    var embeddings = new ImmutableMap.Builder<String, Vector>();
    embeddings.put("aString", vector1);
    embeddings.put("bString", vector2);
    embeddings.put("fString", vector3);
    embeddings.put("arrayString1", vector1);
    embeddings.put("arrayString2", vector2);
    return embeddings.build();
  }

  /**
   * Creates per-field embeddings map from flat embeddings. Each field gets the same embeddings map.
   */
  private ImmutableMap<FieldPath, ImmutableMap<String, Vector>> createEmbeddingsPerField(
      VectorIndexFieldMapping mappings, ImmutableMap<String, Vector> embeddings) {
    ImmutableMap.Builder<FieldPath, ImmutableMap<String, Vector>> builder = ImmutableMap.builder();
    for (FieldPath fieldPath : mappings.fieldMap().keySet()) {
      builder.put(fieldPath, embeddings);
    }
    return builder.build();
  }

  private BsonDocument createEmbeddedBson() {
    BsonDocument bsonDoc = new BsonDocument();
    bsonDoc.append("_id", new BsonString("anId"));
    bsonDoc.append("root", createBasicBson());
    return bsonDoc;
  }

  private BsonDocument createArrayBson() {
    BsonArray bsonArray = new BsonArray();
    bsonArray.add(createBasicBson());
    bsonArray.add(createBasicBson().append("a", new BsonString("aString2")));
    bsonArray.add(new BsonString("arrayString1"));
    bsonArray.add(new BsonString("arrayString2"));
    BsonDocument bsonDoc = new BsonDocument();
    bsonDoc.append("root", bsonArray);
    bsonDoc.append("_id", new BsonString("anId"));
    return bsonDoc;
  }

  private BsonDocument createArrayBsonWithGivenString(String value) {
    BsonArray bsonArray = new BsonArray();
    bsonArray.add(createBasicBson());
    bsonArray.add(createBasicBson().append("a", new BsonString(value)));
    BsonDocument bsonDoc = new BsonDocument();
    bsonDoc.append("root", bsonArray);
    bsonDoc.append("_id", new BsonString("anId"));
    return bsonDoc;
  }

  // Tests for requiresEmbeddingGeneration and extractFilterFieldValues

  @Test
  public void testRequiresEmbeddingGeneration_OnlyFilterFieldsUpdated() {
    // Setup: index with auto-embed field "text" and filter field "color"
    List<VectorIndexFieldDefinition> fields =
        List.of(
            new VectorAutoEmbedFieldDefinition(FieldPath.parse("text")),
            new VectorIndexFilterFieldDefinition(FieldPath.parse("color")));
    VectorIndexFieldMapping mappings = VectorIndexFieldMapping.create(fields);

    // UpdateDescription with only filter field updated
    com.mongodb.client.model.changestream.UpdateDescription updateDescription =
        new com.mongodb.client.model.changestream.UpdateDescription(
            null, new BsonDocument("color", new BsonString("blue")));

    assertFalse(
        AutoEmbeddingDocumentUtils.requiresEmbeddingGeneration(updateDescription, mappings));
  }

  @Test
  public void testRequiresEmbeddingGeneration_AutoEmbedFieldUpdated() {
    // Setup: index with auto-embed field "text" and filter field "color"
    List<VectorIndexFieldDefinition> fields =
        List.of(
            new VectorAutoEmbedFieldDefinition(FieldPath.parse("text")),
            new VectorIndexFilterFieldDefinition(FieldPath.parse("color")));
    VectorIndexFieldMapping mappings = VectorIndexFieldMapping.create(fields);

    // UpdateDescription with auto-embed field updated
    com.mongodb.client.model.changestream.UpdateDescription updateDescription =
        new com.mongodb.client.model.changestream.UpdateDescription(
            null, new BsonDocument("text", new BsonString("new text")));

    assertTrue(AutoEmbeddingDocumentUtils.requiresEmbeddingGeneration(updateDescription, mappings));
  }

  @Test
  public void testRequiresEmbeddingGeneration_BothFieldsUpdated() {
    // Setup: index with auto-embed field "text" and filter field "color"
    List<VectorIndexFieldDefinition> fields =
        List.of(
            new VectorAutoEmbedFieldDefinition(FieldPath.parse("text")),
            new VectorIndexFilterFieldDefinition(FieldPath.parse("color")));
    VectorIndexFieldMapping mappings = VectorIndexFieldMapping.create(fields);

    // UpdateDescription with both fields updated
    com.mongodb.client.model.changestream.UpdateDescription updateDescription =
        new com.mongodb.client.model.changestream.UpdateDescription(
            null,
            new BsonDocument("text", new BsonString("new text"))
                .append("color", new BsonString("blue")));

    assertTrue(AutoEmbeddingDocumentUtils.requiresEmbeddingGeneration(updateDescription, mappings));
  }

  @Test
  public void testRequiresEmbeddingGeneration_AutoEmbedFieldRemoved() {
    // Setup: index with auto-embed field "text" and filter field "color"
    List<VectorIndexFieldDefinition> fields =
        List.of(
            new VectorAutoEmbedFieldDefinition(FieldPath.parse("text")),
            new VectorIndexFilterFieldDefinition(FieldPath.parse("color")));
    VectorIndexFieldMapping mappings = VectorIndexFieldMapping.create(fields);

    // UpdateDescription with auto-embed field removed
    com.mongodb.client.model.changestream.UpdateDescription updateDescription =
        new com.mongodb.client.model.changestream.UpdateDescription(List.of("text"), null);

    assertTrue(AutoEmbeddingDocumentUtils.requiresEmbeddingGeneration(updateDescription, mappings));
  }

  @Test
  public void testRequiresEmbeddingGeneration_FilterFieldRemoved() {
    // Setup: index with auto-embed field "text" and filter field "color"
    List<VectorIndexFieldDefinition> fields =
        List.of(
            new VectorAutoEmbedFieldDefinition(FieldPath.parse("text")),
            new VectorIndexFilterFieldDefinition(FieldPath.parse("color")));
    VectorIndexFieldMapping mappings = VectorIndexFieldMapping.create(fields);

    // UpdateDescription with filter field removed
    com.mongodb.client.model.changestream.UpdateDescription updateDescription =
        new com.mongodb.client.model.changestream.UpdateDescription(List.of("color"), null);

    assertFalse(
        AutoEmbeddingDocumentUtils.requiresEmbeddingGeneration(updateDescription, mappings));
  }

  @Test
  public void testRequiresEmbeddingGeneration_UnrelatedFieldChanged() {
    // Setup: index with auto-embed field "text" and filter field "color"
    List<VectorIndexFieldDefinition> fields =
        List.of(
            new VectorAutoEmbedFieldDefinition(FieldPath.parse("text")),
            new VectorIndexFilterFieldDefinition(FieldPath.parse("color")));
    VectorIndexFieldMapping mappings = VectorIndexFieldMapping.create(fields);

    // UpdateDescription with unrelated field updated (not in index at all)
    com.mongodb.client.model.changestream.UpdateDescription updateDescription =
        new com.mongodb.client.model.changestream.UpdateDescription(
            null, new BsonDocument("unrelated", new BsonString("value")));

    // No AUTO_EMBED/TEXT fields were changed, so embedding is not required.
    // The unrelated field is not in the index, so we don't need to regenerate embeddings.
    assertFalse(
        AutoEmbeddingDocumentUtils.requiresEmbeddingGeneration(updateDescription, mappings));
  }

  @Test
  public void testExtractFilterFieldValues_SingleFilterField() throws IOException {
    // Setup: index with auto-embed field "text" and filter field "color"
    List<VectorIndexFieldDefinition> fields =
        List.of(
            new VectorAutoEmbedFieldDefinition(FieldPath.parse("text")),
            new VectorIndexFilterFieldDefinition(FieldPath.parse("color")));
    VectorIndexFieldMapping mappings = VectorIndexFieldMapping.create(fields);

    // Document with filter field
    BsonDocument bsonDoc =
        new BsonDocument()
            .append("_id", new BsonString("anId"))
            .append("text", new BsonString("some text"))
            .append("color", new BsonString("red"));
    RawBsonDocument rawBsonDoc = new RawBsonDocument(bsonDoc, BsonUtils.BSON_DOCUMENT_CODEC);

    BsonDocument result = AutoEmbeddingDocumentUtils.extractFilterFieldValues(rawBsonDoc, mappings);

    assertEquals(new BsonDocument("color", new BsonString("red")), result);
  }

  @Test
  public void testExtractFilterFieldValues_MultipleFilterFields() throws IOException {
    // Setup: index with auto-embed field "text" and multiple filter fields
    List<VectorIndexFieldDefinition> fields =
        List.of(
            new VectorAutoEmbedFieldDefinition(FieldPath.parse("text")),
            new VectorIndexFilterFieldDefinition(FieldPath.parse("color")),
            new VectorIndexFilterFieldDefinition(FieldPath.parse("size")));
    VectorIndexFieldMapping mappings = VectorIndexFieldMapping.create(fields);

    // Document with multiple filter fields
    BsonDocument bsonDoc =
        new BsonDocument()
            .append("_id", new BsonString("anId"))
            .append("text", new BsonString("some text"))
            .append("color", new BsonString("red"))
            .append("size", new BsonString("large"));
    RawBsonDocument rawBsonDoc = new RawBsonDocument(bsonDoc, BsonUtils.BSON_DOCUMENT_CODEC);

    BsonDocument result = AutoEmbeddingDocumentUtils.extractFilterFieldValues(rawBsonDoc, mappings);

    assertEquals(
        new BsonDocument("color", new BsonString("red")).append("size", new BsonString("large")),
        result);
  }

  @Test
  public void testExtractFilterFieldValues_MissingFilterField() throws IOException {
    // Setup: index with auto-embed field "text" and filter field "color"
    List<VectorIndexFieldDefinition> fields =
        List.of(
            new VectorAutoEmbedFieldDefinition(FieldPath.parse("text")),
            new VectorIndexFilterFieldDefinition(FieldPath.parse("color")));
    VectorIndexFieldMapping mappings = VectorIndexFieldMapping.create(fields);

    // Document without filter field
    BsonDocument bsonDoc =
        new BsonDocument()
            .append("_id", new BsonString("anId"))
            .append("text", new BsonString("some text"));
    RawBsonDocument rawBsonDoc = new RawBsonDocument(bsonDoc, BsonUtils.BSON_DOCUMENT_CODEC);

    BsonDocument result = AutoEmbeddingDocumentUtils.extractFilterFieldValues(rawBsonDoc, mappings);

    // Should return empty document when filter field is missing
    assertEquals(new BsonDocument(), result);
  }

  @Test
  public void extractFilterFieldValues_arrayFilterField_preservesArrayStructure()
      throws IOException {
    // Arrange
    List<VectorIndexFieldDefinition> fields =
        List.of(
            new VectorAutoEmbedFieldDefinition(FieldPath.parse("text")),
            new VectorIndexFilterFieldDefinition(FieldPath.parse("tags")));
    VectorIndexFieldMapping mappings = VectorIndexFieldMapping.create(fields);
    BsonDocument bsonDoc =
        new BsonDocument()
            .append("_id", new BsonString("anId"))
            .append("text", new BsonString("some text"))
            .append(
                "tags",
                new BsonArray(
                    List.of(
                        new BsonString("red"), new BsonString("blue"), new BsonString("green"))));
    RawBsonDocument rawBsonDoc = new RawBsonDocument(bsonDoc, BsonUtils.BSON_DOCUMENT_CODEC);

    // Act
    BsonDocument result = AutoEmbeddingDocumentUtils.extractFilterFieldValues(rawBsonDoc, mappings);

    // Assert
    assertThat(result)
        .isEqualTo(
            new BsonDocument(
                "tags",
                new BsonArray(
                    List.of(
                        new BsonString("red"), new BsonString("blue"), new BsonString("green")))));
  }

  @Test
  public void extractFilterFieldValues_singleElementArray_preservesArrayNotScalar()
      throws IOException {
    // Arrange
    List<VectorIndexFieldDefinition> fields =
        List.of(
            new VectorAutoEmbedFieldDefinition(FieldPath.parse("text")),
            new VectorIndexFilterFieldDefinition(FieldPath.parse("tags")));
    VectorIndexFieldMapping mappings = VectorIndexFieldMapping.create(fields);
    BsonDocument bsonDoc =
        new BsonDocument()
            .append("_id", new BsonString("anId"))
            .append("text", new BsonString("some text"))
            .append("tags", new BsonArray(List.of(new BsonString("red"))));
    RawBsonDocument rawBsonDoc = new RawBsonDocument(bsonDoc, BsonUtils.BSON_DOCUMENT_CODEC);

    // Act
    BsonDocument result = AutoEmbeddingDocumentUtils.extractFilterFieldValues(rawBsonDoc, mappings);

    // Assert
    assertThat(result)
        .isEqualTo(new BsonDocument("tags", new BsonArray(List.of(new BsonString("red")))));
  }

  @Test
  public void extractFilterFieldValues_mixedScalarAndArray_handlesBothCorrectly()
      throws IOException {
    // Arrange
    List<VectorIndexFieldDefinition> fields =
        List.of(
            new VectorAutoEmbedFieldDefinition(FieldPath.parse("text")),
            new VectorIndexFilterFieldDefinition(FieldPath.parse("color")),
            new VectorIndexFilterFieldDefinition(FieldPath.parse("tags")));
    VectorIndexFieldMapping mappings = VectorIndexFieldMapping.create(fields);
    BsonDocument bsonDoc =
        new BsonDocument()
            .append("_id", new BsonString("anId"))
            .append("text", new BsonString("some text"))
            .append("color", new BsonString("red"))
            .append(
                "tags", new BsonArray(List.of(new BsonString("large"), new BsonString("sale"))));
    RawBsonDocument rawBsonDoc = new RawBsonDocument(bsonDoc, BsonUtils.BSON_DOCUMENT_CODEC);

    // Act
    BsonDocument result = AutoEmbeddingDocumentUtils.extractFilterFieldValues(rawBsonDoc, mappings);

    // Assert
    assertThat(result)
        .isEqualTo(
            new BsonDocument("color", new BsonString("red"))
                .append(
                    "tags",
                    new BsonArray(List.of(new BsonString("large"), new BsonString("sale")))));
  }
}
