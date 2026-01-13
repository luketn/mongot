package com.xgen.mongot.index.lucene.explain.information;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.commons.collections4.Equator;
import org.bson.BsonDocument;

public record DisjunctionMaxQuerySpec(List<QueryExplainInformation> disjuncts, Float tieBreaker)
    implements LuceneQuerySpecification {
  static class Fields {
    static final Field.Required<List<QueryExplainInformation>> DISJUNCTS =
        Field.builder("disjuncts")
            .classField(QueryExplainInformation::fromBson)
            .disallowUnknownFields()
            .asList()
            .required();

    static final Field.Required<Float> TIE_BREAKER =
        Field.builder("tieBreaker").floatField().required();
  }

  // returns a version of the DisjunctionMaxQuerySpec where the child QuerySpecs are sorted by a
  // comparator. Useful for testing explain output without worrying about the determinism of
  // the order of Lucene disjunction/conjunctions.
  DisjunctionMaxQuerySpec sorted(Comparator<QueryExplainInformation> comparator) {
    return new DisjunctionMaxQuerySpec(
        QueryExplainInformation.cloneAndSortChildQuerySpecs(this.disjuncts, comparator),
        this.tieBreaker);
  }

  static DisjunctionMaxQuerySpec fromBson(DocumentParser parser) throws BsonParseException {
    return new DisjunctionMaxQuerySpec(
        parser.getField(Fields.DISJUNCTS).unwrap(), parser.getField(Fields.TIE_BREAKER).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.DISJUNCTS, this.disjuncts)
        .field(Fields.TIE_BREAKER, this.tieBreaker)
        .build();
  }

  @Override
  public Type getType() {
    return Type.DISJUNCTION_MAX_QUERY;
  }

  @Override
  public boolean equals(
      LuceneQuerySpecification other,
      Equator<QueryExplainInformation> equator,
      Comparator<QueryExplainInformation> comparator) {
    if (other.getType() != Type.DISJUNCTION_MAX_QUERY) {
      return false;
    }

    DisjunctionMaxQuerySpec otherDisjunctionMax = (DisjunctionMaxQuerySpec) other;
    return Objects.equals(this.tieBreaker, otherDisjunctionMax.tieBreaker)
        && BooleanQuerySpec.clauseListEquals(
            Optional.of(this.disjuncts),
            Optional.of(otherDisjunctionMax.disjuncts),
            equator,
            comparator);
  }
}
