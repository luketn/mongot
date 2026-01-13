package com.xgen.mongot.index.query.operators;

import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.Vector;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import java.util.Optional;
import org.bson.BsonDocument;

public record ExactVectorSearchCriteria(
    FieldPath path,
    Optional<Vector> queryVector,
    Optional<VectorSearchQueryInput> query,
    Optional<VectorSearchFilter> filter,
    int limit,
    boolean returnStoredSource)
    implements VectorSearchCriteria {

  public static ExactVectorSearchCriteria fromBson(
      DocumentParser parser, FieldPath path, VectorSearchFilter.Type filterType)
      throws BsonParseException {
    if (!parser.getField(Fields.EXACT).unwrap()) {
      parser.getContext().handleSemanticError("exact must be true for ExactVectorSearchQuery");
    }

    if (parser.getField(Fields.NUM_CANDIDATES).unwrap().isPresent()) {
      parser
          .getContext()
          .handleSemanticError("\"numCandidates\" must be omitted when \"exact\" is set to true");
    }

    Optional<Vector> vector = parser.getField(Fields.QUERY_VECTOR).unwrap();
    Optional<VectorSearchQueryInput> queryInput = VectorSearchQueryInput.fromBson(parser);

    VectorSearchCriteria.checkBasicFields(parser, vector, queryInput);
    return new ExactVectorSearchCriteria(
        path,
        vector,
        queryInput,
        VectorSearchFilter.fromBson(parser, filterType),
        parser.getField(Fields.LIMIT).unwrap(),
        parser.getField(Fields.STORED_SOURCE).unwrap());
  }

  @Override
  public Type getVectorSearchType() {
    return query().map(ignored -> Type.AUTO_EMBEDDING).orElse(Type.EXACT);
  }

  @Override
  public BsonDocument toBson() {
    BsonDocument filterDocument =
        this.filter.map(DocumentEncodable::toBson).orElse(new BsonDocument());
    BsonDocument queryDocument =
        this.query.map(DocumentEncodable::toBson).orElse(new BsonDocument());
    return BsonDocumentBuilder.builder()
        .field(Fields.LIMIT, this.limit())
        .field(Fields.EXACT, true)
        .field(Fields.QUERY_VECTOR, this.queryVector())
        .field(Fields.STORED_SOURCE, this.returnStoredSource())
        .join(queryDocument)
        .join(filterDocument)
        .build();
  }
}
