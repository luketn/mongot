package com.xgen.mongot.index.query.operators;

import com.xgen.mongot.index.query.scores.Score;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import java.util.List;
import org.bson.BsonDocument;
import org.bson.BsonValue;

/**
 * Represents the {@code vectorSearch} operator in a $search query.
 *
 * <p>This operator encapsulates the scoring configuration and {@code VectorSearchCriteria} to
 * provide feature parity with $vectorSearch query.
 *
 * @param score score specification
 * @param criteria vector search criteria
 */
public record VectorSearchOperator(Score score, VectorSearchCriteria criteria) implements Operator {

  @Override
  public Type getType() {
    return Type.VECTOR_SEARCH;
  }

  @Override
  public BsonValue operatorToBson() {
    BsonDocument criteriaDoc = this.criteria.toBson();
    return Operators.documentBuilder(score(), List.of(this.criteria.path()))
        .join(criteriaDoc)
        .build();
  }

  public static VectorSearchOperator fromBson(DocumentParser parser) throws BsonParseException {
    if (parser.getContext().getHierarchy().size() > 1) {
      parser.getContext().handleSemanticError("vectorSearch is not allowed to be nested");
    }

    List<FieldPath> unwrap = parser.getField(Operators.Fields.FIELD_PATH).unwrap();
    if (unwrap.size() != 1) {
      parser.getContext().handleSemanticError("vectorSearch can have only one field path");
    }
    return new VectorSearchOperator(
        Operators.parseScore(parser),
        VectorSearchCriteria.fromBson(parser, unwrap.get(0), VectorSearchFilter.Type.OPERATOR));
  }
}
