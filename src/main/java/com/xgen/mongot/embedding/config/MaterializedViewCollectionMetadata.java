package com.xgen.mongot.embedding.config;

import static com.google.common.collect.ImmutableMap.toImmutableMap;

import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.Value;
import java.util.Map;
import java.util.UUID;
import org.bson.BsonDocument;

/**
 * Runtime data model for MaterializedView Collection Metadata, represents immutable properties of a
 * MaterializedView collection used for indexing and query.
 */
public record MaterializedViewCollectionMetadata(
    MaterializedViewSchemaMetadata schemaMetadata, UUID collectionUuid, String collectionName)
    implements DocumentEncodable {

  public static MaterializedViewCollectionMetadata fromBson(DocumentParser parser)
      throws BsonParseException {
    return new MaterializedViewCollectionMetadata(
        parser.getField(Fields.MV_SCHEMA_METADATA).unwrap(),
        parser.getField(Fields.COLLECTION_UUID).unwrap(),
        parser.getField(Fields.COLLECTION_NAME).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.MV_SCHEMA_METADATA, this.schemaMetadata)
        .field(Fields.COLLECTION_UUID, this.collectionUuid)
        .field(Fields.COLLECTION_NAME, this.collectionName)
        .build();
  }

  /** Materialized View Schema metadata for both indexing and query. */
  public record MaterializedViewSchemaMetadata(
      long materializedViewSchemaVersion, Map<FieldPath, FieldPath> autoEmbeddingFieldsMapping)
      implements DocumentEncodable {

    /** Fallback version for community users. */
    public static final MaterializedViewSchemaMetadata VERSION_ZERO =
        new MaterializedViewSchemaMetadata(0L, Map.of());

    public static MaterializedViewSchemaMetadata fromBson(DocumentParser parser)
        throws BsonParseException {
      return new MaterializedViewSchemaMetadata(
          parser.getField(Fields.MV_METADATA_SCHEMA_VERSION).unwrap(),
          parser.getField(Fields.SCHEMA_FIELD_MAPPING).unwrap().entrySet().stream()
              .collect(
                  toImmutableMap(
                      e -> FieldPath.parse(e.getKey()), e -> FieldPath.parse(e.getValue()))));
    }

    @Override
    public BsonDocument toBson() {
      return BsonDocumentBuilder.builder()
          .field(
              Fields.SCHEMA_FIELD_MAPPING,
              this.autoEmbeddingFieldsMapping.entrySet().stream()
                  .collect(
                      toImmutableMap(e -> e.getKey().toString(), e -> e.getValue().toString())))
          .field(Fields.MV_METADATA_SCHEMA_VERSION, this.materializedViewSchemaVersion)
          .build();
    }

    private static class Fields {
      /**
       * Field path mapping from user source collection to materialized view collection. e.g,
       * {'title': '_autoEmbed.title'}
       */
      public static final Field.Required<Map<String, String>> SCHEMA_FIELD_MAPPING =
          Field.builder("schemaFieldMapping")
              .mapOf(Value.builder().stringValue().required())
              .required();

      /** The schema version of the materialized view metadata, not Lease schema version. */
      public static final Field.Required<Long> MV_METADATA_SCHEMA_VERSION =
          Field.builder("mvMetadataSchemaVersion").longField().required();
    }
  }

  private static class Fields {
    /** The UUID of the collection that the index is on. */
    public static final Field.Required<UUID> COLLECTION_UUID =
        Field.builder("collectionUuid").uuidField().encodeAsString().required();

    /** The name of the collection that the index is on. */
    public static final Field.Required<String> COLLECTION_NAME =
        Field.builder("collectionName").stringField().required();

    public static final Field.Required<MaterializedViewSchemaMetadata> MV_SCHEMA_METADATA =
        Field.builder("mvSchemaMetadata")
            .classField(MaterializedViewSchemaMetadata::fromBson)
            .allowUnknownFields()
            .required();
  }
}
