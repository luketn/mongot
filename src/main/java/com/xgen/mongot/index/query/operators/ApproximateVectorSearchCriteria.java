package com.xgen.mongot.index.query.operators;

import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.Vector;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import java.util.Optional;
import org.bson.BsonDocument;

public record ApproximateVectorSearchCriteria(
    FieldPath path,
    Optional<Vector> queryVector,
    Optional<VectorSearchQueryInput> query,
    Optional<VectorSearchFilter> filter,
    int limit,
    int numCandidates,
    Optional<ExplainOptions> explainOptions,
    boolean returnStoredSource)
    implements VectorSearchCriteria {

  public ApproximateVectorSearchCriteria {
    Check.checkArg(limit <= numCandidates, "limit should be less than or equal to numCandidates");
  }

  public static ApproximateVectorSearchCriteria fromBson(
      DocumentParser parser, FieldPath path, VectorSearchFilter.Type filterType)
      throws BsonParseException {
    boolean exact = parser.getField(Fields.EXACT).unwrap();
    if (exact) {
      throw new BsonParseException(
          "exact must be false or omitted for ApproximateVectorSearchCriteria", Optional.empty());
    }
    Optional<Integer> wrappedNumCandidates = parser.getField(Fields.NUM_CANDIDATES).unwrap();
    if (wrappedNumCandidates.isEmpty()) {
      parser
          .getContext()
          .handleSemanticError("numCandidates is required for approximate vector search");
    }
    int numCandidates = wrappedNumCandidates.get();
    int limit = parser.getField(Fields.LIMIT).unwrap();
    if (limit > numCandidates) {
      parser
          .getContext()
          .handleSemanticError("limit should be less than or equal to numCandidates");
    }
    var vector = parser.getField(Fields.QUERY_VECTOR).unwrap();
    var queryInput = VectorSearchQueryInput.fromBson(parser);

    VectorSearchCriteria.checkBasicFields(parser, vector, queryInput);

    return new ApproximateVectorSearchCriteria(
        path,
        vector,
        queryInput,
        VectorSearchFilter.fromBson(parser, filterType),
        limit,
        numCandidates,
        parser.getField(Fields.EXPLAIN_OPTIONS).unwrap(),
        parser.getField(Fields.STORED_SOURCE).unwrap());
  }

  @Override
  public Type getVectorSearchType() {
    return query().map(ignored -> Type.AUTO_EMBEDDING).orElse(Type.APPROXIMATE);
  }

  @Override
  public BsonDocument toBson() {
    BsonDocument filterDocument =
        this.filter.map(DocumentEncodable::toBson).orElse(new BsonDocument());
    BsonDocument queryDocument =
        this.query.map(DocumentEncodable::toBson).orElse(new BsonDocument());
    return BsonDocumentBuilder.builder()
        .field(Fields.LIMIT, this.limit())
        .field(Fields.EXACT, false)
        .field(Fields.NUM_CANDIDATES, Optional.of(this.numCandidates))
        .field(Fields.QUERY_VECTOR, this.queryVector())
        .field(Fields.EXPLAIN_OPTIONS, this.explainOptions())
        .field(Fields.STORED_SOURCE, this.returnStoredSource())
        .join(queryDocument)
        .join(filterDocument)
        .build();
  }
}
