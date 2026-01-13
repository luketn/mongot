package com.xgen.mongot.index.lucene.explain.information;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Encodable;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonValue;

public record VectorSearchTracingSpec(
    BsonValue documentId,
    boolean unreachable,
    boolean hasNoVector,
    boolean visited,
    Optional<Float> vectorSearchScore,
    Optional<String> luceneSegment,
    Optional<DropReason> dropReason)
    implements Encodable {

  private static class Fields {
    static final Field.Required<BsonValue> DOCUMENT_ID =
        Field.builder("documentId").unparsedValueField().required();
    static final Field.WithDefault<Boolean> UNREACHABLE =
        Field.builder("unreachable").booleanField().optional().withDefault(false);
    static final Field.WithDefault<Boolean> HAS_NO_VECTOR =
        Field.builder("hasNoVector").booleanField().optional().withDefault(false);
    static final Field.Required<Boolean> VISITED =
        Field.builder("visited").booleanField().required();
    static final Field.Optional<Float> VECTOR_SEARCH_SCORE =
        Field.builder("vectorSearchScore").floatField().optional().noDefault();
    static final Field.Optional<String> LUCENE_SEGMENT =
        Field.builder("luceneSegment").stringField().optional().noDefault();
    static final Field.Optional<DropReason> DROP_REASON =
        Field.builder("dropReason")
            .enumField(DropReason.class)
            .asUpperCamelCase()
            .optional()
            .noDefault();
  }

  public enum DropReason {
    FILTER, // The document was filtered out
    NON_COMPETITIVE_SCORE, // The document’s score was not competitive within its segment
    MERGE, // The document’s score was not competitive during merge
    LIMIT_CAP, // The document fit within numCandidates but was dropped due to the limit cap
    RESCORING // The document was kicked out at rescoring stage
  }

  public static VectorSearchTracingSpec fromBson(DocumentParser parser) throws BsonParseException {
    return new VectorSearchTracingSpec(
        parser.getField(Fields.DOCUMENT_ID).unwrap(),
        parser.getField(Fields.UNREACHABLE).unwrap(),
        parser.getField(Fields.HAS_NO_VECTOR).unwrap(),
        parser.getField(Fields.VISITED).unwrap(),
        parser.getField(Fields.VECTOR_SEARCH_SCORE).unwrap(),
        parser.getField(Fields.LUCENE_SEGMENT).unwrap(),
        parser.getField(Fields.DROP_REASON).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.DOCUMENT_ID, this.documentId)
        .fieldOmitDefaultValue(Fields.UNREACHABLE, this.unreachable)
        .fieldOmitDefaultValue(Fields.HAS_NO_VECTOR, this.hasNoVector)
        .field(Fields.VISITED, this.visited)
        .field(Fields.VECTOR_SEARCH_SCORE, this.vectorSearchScore)
        .field(Fields.LUCENE_SEGMENT, this.luceneSegment)
        .field(Fields.DROP_REASON, this.dropReason)
        .build();
  }
}
