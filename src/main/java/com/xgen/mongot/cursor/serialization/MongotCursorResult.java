package com.xgen.mongot.cursor.serialization;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonValue;

/**
 * Represents a batch of results for a given cursor. It contains the cursor ID, a batch of results,
 * and the namespace of the results. For an intermediate query, this will also contain a type that
 * specifies whether these are metadata results or search results.
 *
 * <pre>
 *   {
 *     "id": 12345,
 *     "nextBatch": [...],
 *     "namespace": "db.collection",
 *     "type": "meta/results"
 *   }
 * </pre>
 */
public record MongotCursorResult(long id, BsonValue batch, String namespace, Optional<Type> type)
    implements DocumentEncodable {
  public static final int EXHAUSTED_CURSOR_ID = 0;

  private static class Fields {
    private static final Field.Required<Long> ID = Field.builder("id").longField().required();

    private static final Field.Required<BsonValue> NEXT_BATCH =
        Field.builder("nextBatch").unparsedValueField().required();

    private static final Field.Required<String> NAMESPACE =
        Field.builder("ns").stringField().required();

    private static final Field.Optional<Type> TYPE =
        Field.builder("type").enumField(Type.class).asCamelCase().optional().noDefault();
  }

  public enum Type {
    RESULTS,
    META
  }

  public static MongotCursorResult fromBson(DocumentParser parser) throws BsonParseException {
    return new MongotCursorResult(
        parser.getField(Fields.ID).unwrap(),
        parser.getField(Fields.NEXT_BATCH).unwrap(),
        parser.getField(Fields.NAMESPACE).unwrap(),
        parser.getField(Fields.TYPE).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.ID, this.id)
        .field(Fields.NEXT_BATCH, this.batch)
        .field(Fields.NAMESPACE, this.namespace)
        .field(Fields.TYPE, this.type)
        .build();
  }

  public long getCursorId() {
    return this.id;
  }

  public BsonValue getBatch() {
    return this.batch;
  }

  public String getNamespace() {
    return this.namespace;
  }

  public Optional<Type> getType() {
    return this.type;
  }
}
