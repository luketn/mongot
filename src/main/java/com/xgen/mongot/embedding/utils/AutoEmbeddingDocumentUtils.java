package com.xgen.mongot.embedding.utils;

import static com.xgen.mongot.embedding.utils.AutoEmbeddingIndexDefinitionUtils.getHashFieldPath;

import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.Var;
import com.mongodb.client.model.changestream.UpdateDescription;
import com.xgen.mongot.index.DocumentEvent;
import com.xgen.mongot.index.definition.VectorIndexFieldDefinition;
import com.xgen.mongot.index.definition.VectorIndexFieldMapping;
import com.xgen.mongot.index.ingestion.BsonDocumentProcessor;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.BsonVectorParser;
import com.xgen.mongot.util.bson.Vector;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AutoEmbeddingDocumentUtils {

  private static final Logger LOG = LoggerFactory.getLogger(AutoEmbeddingDocumentUtils.class);

  /**
   * Extracts string field values from rawBsonDocument based on VectorIndexFieldMappings (from index
   * definition), only processes string field paths defined in VectorTextFieldDefinition. Text
   * strings are mapped by field path to support multi-model index. Returns a FieldPath to Strings
   * mapping for auto-embedding calls.
   */
  public static ImmutableMap<FieldPath, Set<String>> getVectorTextPathMap(
      RawBsonDocument rawBsonDocument, VectorIndexFieldMapping fieldMapping) throws IOException {
    CollectVectorTextStringsDocumentHandler handler =
        CollectVectorTextStringsDocumentHandler.create(fieldMapping, Optional.empty());
    BsonDocumentProcessor.process(rawBsonDocument, handler);
    return handler.getVectorTextPathMap();
  }

  /**
   * Constructs a new DocumentEvent by adding per document embeddings from a large embeddings
   * inputs. Only embeddings with matched field path and string text value will be added into
   * DocumentEvent.
   */
  public static DocumentEvent buildAutoEmbeddingDocumentEvent(
      DocumentEvent rawDocumentEvent,
      VectorIndexFieldMapping fieldMapping,
      ImmutableMap<FieldPath, ImmutableMap<String, Vector>> allVectorsFromBatchResponse)
      throws IOException {
    if (rawDocumentEvent.getDocument().isEmpty()) {
      return rawDocumentEvent;
    }
    ImmutableMap<FieldPath, Set<String>> textPerFieldInDoc =
        getVectorTextPathMap(rawDocumentEvent.getDocument().get(), fieldMapping);
    // Output vector map builder to load filtered vectors from allVectorsFromBatchRequest
    ImmutableMap.Builder<FieldPath, ImmutableMap<String, Vector>> filteredVectorMapBuilder =
        ImmutableMap.builder();
    for (var fieldAndTexts : textPerFieldInDoc.entrySet()) {
      if (!allVectorsFromBatchResponse.containsKey(fieldAndTexts.getKey())) {
        continue;
      }
      ImmutableMap<String, Vector> sourceEmbeddings =
          allVectorsFromBatchResponse.get(fieldAndTexts.getKey());
      ImmutableMap.Builder<String, Vector> filteredEmbeddingBuilder = ImmutableMap.builder();
      for (String text : fieldAndTexts.getValue()) {
        if (sourceEmbeddings.containsKey(text)) {
          filteredEmbeddingBuilder.put(text, sourceEmbeddings.get(text));
        }
        // missing embedding won't be handled here, but will have
        // LuceneVectorIndexFieldValueHandler::handleString to skip building those auto-embedding
        // text field in DocumentBuilder
      }
      filteredVectorMapBuilder.put(fieldAndTexts.getKey(), filteredEmbeddingBuilder.build());
    }
    return DocumentEvent.createFromDocumentEventAndVectors(
        rawDocumentEvent, filteredVectorMapBuilder.build());
  }

  /**
   * Constructs a new document, replaces given string fields by looking up fieldPath,
   * string-to-vector mappings in the embeddingsPerField map, skips the string field if string value
   * is not found in embeddingsPerField map. It only replaces fields defined in
   * VectorTextFieldDefinition, keeps all other fields unchanged. Also computes and inserts SHA-256
   * hashes of the original text values for each embedded field.
   */
  public static DocumentEvent buildMaterializedViewDocumentEvent(
      DocumentEvent rawDocumentEvent,
      VectorIndexFieldMapping fieldMapping,
      ImmutableMap<FieldPath, ImmutableMap<String, Vector>> embeddingsPerField)
      throws IOException {
    if (rawDocumentEvent.getDocument().isEmpty()) {
      return rawDocumentEvent;
    }
    BsonDocument bsonDoc = new BsonDocument();
    ReplaceStringsDocumentHandler handler =
        ReplaceStringsDocumentHandler.create(
            fieldMapping,
            Optional.empty(),
            bsonDoc,
            embeddingsPerField,
            rawDocumentEvent.getAutoEmbeddings());
    BsonDocumentProcessor.process(rawDocumentEvent.getDocument().get(), handler);

    return DocumentEvent.createFromDocumentEvent(
        rawDocumentEvent, new RawBsonDocument(bsonDoc, BsonUtils.BSON_DOCUMENT_CODEC));
  }

  /**
   * Compares a source collection document with a materialized view document to determine if it
   * needs to be re-indexed. The comparison is a multi-step process:
   *
   * <ul>
   *   <li>1. Compare filter fields. If any filter field values don't match, the document needs to
   *       be re-indexed.
   *   <li>2. Compare auto-embedding fields. If any auto-embedding fields don't match, the document
   *       needs to be re-indexed. For this, we rely on the hashes of the auto-embedding fields.
   *   <li>3. Check if the materialized view document has any extra fields that are no longer in the
   *       index definition. If so, the document needs to be re-indexed.
   * </ul>
   *
   * @return A DocumentComparisonResult object containing a boolean indicating whether the document
   *     needs to be re-indexed, and a map of reusable embeddings.
   */
  public static DocumentComparisonResult compareDocuments(
      RawBsonDocument sourceDoc,
      RawBsonDocument matViewDoc,
      VectorIndexFieldMapping mappings,
      VectorIndexFieldMapping matViewMappings) {
    // Collect all autoEmbed and filter fields from source doc.
    var sourceFilterValuesCollector =
        CollectFieldValueDocumentHandler.create(
            mappings, Optional.empty(), (fieldDefinition) -> true, true);

    // Collect all fields from mat view doc, even if they are not in the index definition.
    var matViewFilterValuesCollector =
        CollectFieldValueDocumentHandler.create(
            matViewMappings, Optional.empty(), (fieldDefinition) -> true, false);

    @Var boolean needsReIndexing = false;
    var reusableEmbeddingsBuilder = ImmutableMap.<FieldPath, ImmutableMap<String, Vector>>builder();

    try {
      BsonDocumentProcessor.process(sourceDoc, sourceFilterValuesCollector);
      BsonDocumentProcessor.process(matViewDoc, matViewFilterValuesCollector);

      var sourceDocValues = sourceFilterValuesCollector.getCollectedValues();
      var matViewValues = matViewFilterValuesCollector.getCollectedValues();

      for (Map.Entry<FieldPath, VectorIndexFieldDefinition> entry :
          mappings.fieldMap().entrySet()) {
        FieldPath fieldPath = entry.getKey();
        VectorIndexFieldDefinition fieldDefinition = entry.getValue();

        // Check filter values = here we do a simple quality check.
        if (fieldDefinition.getType() == VectorIndexFieldDefinition.Type.FILTER) {
          if (!Objects.equals(sourceDocValues.get(fieldPath), matViewValues.get(fieldPath))) {
            needsReIndexing = true;
          }
        }

        var reusableEmbeddingsForFieldBuilder = ImmutableMap.<String, Vector>builder();

        // check embeddings against their hashes. Collect ones that match for re-use.
        if (fieldDefinition.getType() == VectorIndexFieldDefinition.Type.AUTO_EMBED) {
          if (sourceDocValues.containsKey(fieldPath)) {
            var stringValuesWithHashes =
                sourceDocValues.get(fieldPath).stream()
                    .filter(BsonValue::isString)
                    .collect(
                        Collectors.toMap(
                            key -> key.asString().getValue(),
                            value ->
                                ReplaceStringsFieldValueHandler.computeTextHash(
                                    value.asString().getValue()),
                            (a, b) -> a));

            // Count non-empty strings in source
            long nonEmptySourceCount = stringValuesWithHashes.keySet().stream()
                .filter(s -> !s.isEmpty())
                .count();

            // Empty strings are not embedded, so skip checking the field if all values are empty.
            // The scenario where we have an array field and some values in the array are empty is
            // handled separately later below.
            if (stringValuesWithHashes.keySet().stream().allMatch(String::isEmpty)) {
              // Case where mat view has embeddings for this field, but source doc has an empty
              // string value.
              if (matViewValues.containsKey(fieldPath)) {
                needsReIndexing = true;
              }
              continue;
            }
            if (matViewValues.containsKey(fieldPath)) {
              var matViewHashes =
                  matViewValues.get(getHashFieldPath(fieldPath)).stream()
                      .map(value -> value.asString().getValue())
                      .toList();
              var matViewVectors =
                  matViewValues.get(fieldPath).stream().map(BsonValue::asBinary).toList();
              for (var stringHashEntry : stringValuesWithHashes.entrySet()) {
                // This is the case where we have an array field and some values are empty.
                if (stringHashEntry.getKey().isEmpty()) {
                  continue;
                }
                var index = matViewHashes.indexOf(stringHashEntry.getValue());
                if (index == -1) {
                  needsReIndexing = true;
                } else {
                  reusableEmbeddingsForFieldBuilder.put(
                      stringHashEntry.getKey(), BsonVectorParser.parse(matViewVectors.get(index)));
                }
              }

              // Check if the number of non-empty strings in source matches the number of
              // embeddings.
              if (nonEmptySourceCount != matViewVectors.size()) {
                needsReIndexing = true;
              }
            } else {
              needsReIndexing = true;
            }
          } else {
            // Case where mat view has embeddings for this field, but source doc does not have this
            // field at all.
            if (matViewValues.containsKey(fieldPath)) {
              needsReIndexing = true;
            }
          }
          var reusableEmbeddingsForField = reusableEmbeddingsForFieldBuilder.build();
          if (!reusableEmbeddingsForField.isEmpty()) {
            reusableEmbeddingsBuilder.put(fieldPath, reusableEmbeddingsForField);
          }
        }
      }

      // check if mat view has extra fields which are no longer in the index definition.
      // an example of this is when a filter field is removed.
      for (FieldPath fieldPath : matViewValues.keySet()) {
        if (!fieldPath.toString().equals("_id")
            && !matViewMappings.fieldMap().containsKey(fieldPath)) {
          needsReIndexing = true;
          break;
        }
      }
    } catch (Exception e) {
      LOG.warn("Caught exception comparing documents, re-indexing.", e);
      needsReIndexing = true;
    }
    return new DocumentComparisonResult(needsReIndexing, reusableEmbeddingsBuilder.build());
  }

  public record DocumentComparisonResult(
      boolean needsReIndexing,
      ImmutableMap<FieldPath, ImmutableMap<String, Vector>> reusableEmbeddings) {
  }

  /**
   * Determines if an update requires embedding generation (i.e., any AUTO_EMBED or TEXT field
   * changed). When this returns false, we can skip the embedding service call and use a partial
   * update for filter-only changes.
   *
   * <p>This method uses positive assertion (checking if embedding IS required) rather than
   * negative assertion (checking if it's NOT required) for fail-safe behavior: if there's a bug, we
   * make extra embedding calls (performance issue) rather than skip necessary ones (data
   * correctness issue).
   *
   * <p>Note: Fields not in the index definition (non-indexed fields) are treated the same as
   * filter fields - they don't require embedding generation. These fields are simply not propagated
   * to the materialized view.
   *
   * @param updateDescription the update description from the change stream event
   * @param fieldMapping      the vector index field mapping
   * @return true if any AUTO_EMBED/TEXT fields changed and embedding is required, false if only
   *     filter fields or non-indexed fields were updated
   */
  public static boolean requiresEmbeddingGeneration(
      UpdateDescription updateDescription, VectorIndexFieldMapping fieldMapping) {
    if (updateDescription == null) {
      // No update description means we can't determine what changed - require embedding to be safe
      return true;
    }

    List<String> removedFields =
        Optional.ofNullable(updateDescription.getRemovedFields()).orElseGet(Collections::emptyList);

    Set<String> updatedFields =
        Optional.ofNullable(updateDescription.getUpdatedFields())
            .map(BsonDocument::keySet)
            .orElseGet(Collections::emptySet);

    // If no fields were updated or removed, require embedding to be safe
    if (removedFields.isEmpty() && updatedFields.isEmpty()) {
      return true;
    }

    // Check if any updated field is an AUTO_EMBED or TEXT field
    for (String field : updatedFields) {
      if (fieldMapping
          .getFieldDefinition(FieldPath.parse(field))
          .filter(VectorIndexFieldDefinition::isAutoEmbedField)
          .isPresent()) {
        return true;
      }
    }

    // Check if any removed field is an AUTO_EMBED or TEXT field
    for (String field : removedFields) {
      if (fieldMapping
          .getFieldDefinition(FieldPath.parse(field))
          .filter(VectorIndexFieldDefinition::isAutoEmbedField)
          .isPresent()) {
        return true;
      }
    }

    // No AUTO_EMBED/TEXT fields changed - only filter fields were updated
    return false;
  }

  /**
   * Extracts filter field values from the full document to build a $set document for partial
   * updates. Only includes fields that are defined as FILTER in the index definition.
   *
   * @param fullDocument the full document from the change stream event
   * @param fieldMapping the vector index field mapping
   * @return a BsonDocument containing only the filter field values for use with $set
   */
  public static BsonDocument extractFilterFieldValues(
      RawBsonDocument fullDocument, VectorIndexFieldMapping fieldMapping) {
    try {
      var filterValuesCollector =
          CollectFieldValueDocumentHandler.create(
              fieldMapping,
              Optional.empty(),
              fieldDef -> fieldDef.getType() == VectorIndexFieldDefinition.Type.FILTER,
              true);

      BsonDocumentProcessor.process(fullDocument, filterValuesCollector);
      return filterValuesCollector.toBsonDocument();
    } catch (Exception e) {
      LOG.warn(
          "Failed to extract filter field values, falling back to full document processing", e);
      return new BsonDocument();
    }
  }
}
