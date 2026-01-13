package com.xgen.mongot.index.synonym;

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.Var;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.ListField;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.bson.BsonBinarySubType;
import org.bson.BsonDocument;
import org.bson.BsonValue;

/**
 * Serialization and validation for a single synonym document in a synonym source collection. See
 * the <a href="https://github.com/10gen/mongot/blob/master/docs/syntax/synonym-collection.md">
 * syntax document</a> for full details.
 */
public class SynonymDocument implements DocumentEncodable {

  static final class Fields {
    static final Field.Required<MappingType> MAPPING_TYPE =
        Field.builder("mappingType").enumField(MappingType.class).asCamelCase().required();

    static final Field.Required<List<String>> SYNONYMS =
        Field.builder("synonyms")
            .stringField()
            .mustNotBeBlank()
            .asList()
            .mustNotBeEmpty()
            .required();

    private static final ListField.FieldBuilder<String> INPUT_BUILDER =
        Field.builder("input").stringField().mustNotBeBlank().asList().mustNotBeEmpty();

    /**
     * Use OPTIONAL_INPUT when serializing synonym documents, because INPUT is an optional field for
     * equivalent synonym documents.
     */
    static final Field.Optional<List<String>> OPTIONAL_INPUT = INPUT_BUILDER.optional().noDefault();

    /**
     * Use REQUIRED_INPUT after validating that type is EXPLICIT during deserialization, because
     * INPUT is a required field for explicit synonym documents.
     */
    static final Field.Required<List<String>> REQUIRED_INPUT = INPUT_BUILDER.required();
  }

  private static class IdField {
    static final class Fields {
      // UNPARSED_VALUE only used to deserialize several different bson types and convert them to
      // string
      static final Field.Optional<BsonValue> UNPARSED_VALUE =
          Field.builder("_id").unparsedValueField().optional().noDefault();
      // PARSED_STRING only used to serialize _id to a representative string
      static final Field.Optional<String> PARSED_STRING =
          Field.builder("_id").stringField().optional().noDefault();
    }
  }

  @VisibleForTesting
  public static Optional<String> idStringFromBsonValue(Optional<BsonValue> value) {
    return value.flatMap(
        bsonValue ->
            switch (bsonValue.getBsonType()) {
              case STRING -> Optional.of(bsonValue.asString().getValue());
              case DOUBLE -> Optional.of(String.valueOf(bsonValue.asDouble().getValue()));
              case INT32 -> Optional.of(String.valueOf(bsonValue.asInt32().getValue()));
              case INT64 -> Optional.of(String.valueOf(bsonValue.asInt64().getValue()));
              case OBJECT_ID -> Optional.of(bsonValue.asObjectId().getValue().toString());
              case BINARY ->
                  BsonBinarySubType.isUuid(bsonValue.asBinary().getType())
                      ? Optional.of(bsonValue.asBinary().asUuid().toString())
                      : Optional.empty();

              default -> Optional.empty();
            });
  }

  public static Optional<String> idStringFromBson(DocumentParser parser) throws BsonParseException {
    return idStringFromBsonValue(parser.getField(IdField.Fields.UNPARSED_VALUE).unwrap());
  }

  public enum MappingType {
    EQUIVALENT,
    EXPLICIT,
  }

  private final MappingType mappingType;
  private final List<String> synonyms;
  private final Optional<List<String>> input;
  private final Optional<String> docId;

  SynonymDocument(
      MappingType mappingType,
      List<String> synonyms,
      Optional<List<String>> input,
      Optional<String> docId) {
    this.mappingType = mappingType;
    this.synonyms = synonyms;
    this.input = input;
    this.docId = docId;
  }

  public static SynonymDocument create(
      MappingType mappingType,
      List<String> synonyms,
      Optional<List<String>> input,
      Optional<String> docId) {
    return new SynonymDocument(mappingType, synonyms, input, docId);
  }

  // Used for testing only. Throws error when docId deserialization fails.
  @VisibleForTesting
  static SynonymDocument fromTestBson(BsonDocument document) throws SynonymMappingException {
    try (var parser = BsonDocumentParser.fromRoot(document).allowUnknownFields(true).build()) {
      Optional<String> docId = idStringFromBson(parser);
      return fromBson(parser, docId);
    } catch (BsonParseException e) {
      throw SynonymMappingException.invalidSynonymDocument(e);
    }
  }

  /** Deserialize a SynonymDocument from BSON. */
  public static SynonymDocument fromBson(BsonDocument document) throws SynonymMappingException {
    @Var Optional<String> docId = Optional.empty();
    try (var parser = BsonDocumentParser.fromRoot(document).allowUnknownFields(true).build()) {
      docId = getIdIfPresent(parser);
      return fromBson(parser, docId);
    } catch (BsonParseException e) {
      throw SynonymMappingException.invalidSynonymDocument(docId, e);
    }
  }

  /**
   * Deserialize a SynonymDocument. This method private to ensure allowUnknownFields is configured
   * to be true on parser, as done in {@link SynonymDocument#fromBson(BsonDocument)}.
   */
  private static SynonymDocument fromBson(DocumentParser parser, Optional<String> docId)
      throws BsonParseException {
    return switch (parser.getField(Fields.MAPPING_TYPE).unwrap()) {
      case EQUIVALENT ->
          // if mappingType is EQUIVALENT, the "input" field is not considered part of this synonym
          // document and is not validated.
          SynonymDocument.create(
              MappingType.EQUIVALENT,
              parser.getField(Fields.SYNONYMS).unwrap(),
              Optional.empty(),
              docId);
      case EXPLICIT ->
          SynonymDocument.create(
              MappingType.EXPLICIT,
              parser.getField(Fields.SYNONYMS).unwrap(),
              Optional.of(parser.getField(Fields.REQUIRED_INPUT).unwrap()),
              docId);
    };
  }

  private static Optional<String> getIdIfPresent(BsonDocumentParser parser) {
    // best-effort deserialization of _id to use in error message when available
    try {
      // only use non-blank docIds in error messages, do not deserialize otherwise
      return idStringFromBson(parser);
    } catch (Exception ignored) {
      // swallow all exceptions related to deserializing _id. _id is not required for synonym
      // mapping creation
    }
    return Optional.empty();
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(IdField.Fields.PARSED_STRING, this.docId)
        .field(Fields.MAPPING_TYPE, this.mappingType)
        .field(Fields.SYNONYMS, this.synonyms)
        .field(Fields.OPTIONAL_INPUT, this.input)
        .build();
  }

  public MappingType getMappingType() {
    return this.mappingType;
  }

  public List<String> getSynonyms() {
    return this.synonyms;
  }

  public Optional<List<String>> getInput() {
    return this.input;
  }

  public Optional<String> getDocId() {
    return this.docId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SynonymDocument that = (SynonymDocument) o;
    return this.mappingType == that.mappingType
        && this.synonyms.equals(that.synonyms)
        && this.input.equals(that.input)
        && this.docId.equals(that.docId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.mappingType, this.synonyms, this.input, this.docId);
  }
}
