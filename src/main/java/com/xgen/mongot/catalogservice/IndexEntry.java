package com.xgen.mongot.catalogservice;

import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Objects;
import org.bson.BsonDocument;
import org.bson.types.ObjectId;

record IndexEntry(
    AuthoritativeIndexKey indexKey, ObjectId indexId, int schemaVersion, IndexDefinition definition)
    implements DocumentEncodable {

  IndexEntry {
    Objects.requireNonNull(indexKey);
    Objects.requireNonNull(indexId);
    Objects.requireNonNull(definition);
  }

  abstract static class Fields {
    static final Field.Required<AuthoritativeIndexKey> INDEX_KEY =
        Field.builder("_id")
            .classField(AuthoritativeIndexKey::fromBson)
            .disallowUnknownFields()
            .required();

    static final Field.Required<ObjectId> ID = Field.builder("indexId").objectIdField().required();
    static final Field.Required<Integer> VERSION =
        Field.builder("schemaVersion").intField().required();

    static final Field.Required<IndexDefinition> DEFINITION =
        Field.builder("definition")
            .classField(IndexDefinition::fromBson)
            .disallowUnknownFields()
            .required();
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.INDEX_KEY, this.indexKey())
        .field(Fields.ID, this.indexId())
        .field(Fields.VERSION, this.schemaVersion())
        .field(Fields.DEFINITION, this.definition())
        .build();
  }

  public BsonDocument keyBson() {
    return keyAsBson(this.indexKey);
  }

  public static BsonDocument keyAsBson(AuthoritativeIndexKey indexKey) {
    return BsonDocumentBuilder.builder().field(Fields.INDEX_KEY, indexKey).build();
  }

  public static IndexEntry fromBson(DocumentParser parser) throws BsonParseException {
    return new IndexEntry(
        parser.getField(Fields.INDEX_KEY).unwrap(),
        parser.getField(Fields.ID).unwrap(),
        parser.getField(Fields.VERSION).unwrap(),
        parser.getField(Fields.DEFINITION).unwrap());
  }
}
