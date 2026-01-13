package com.xgen.mongot.index.lucene.explain.information;

import com.xgen.mongot.util.Equality;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import org.apache.commons.collections4.Equator;
import org.bson.BsonDocument;

public record BooleanQuerySpec(
    Optional<List<QueryExplainInformation>> must,
    Optional<List<QueryExplainInformation>> mustNot,
    Optional<List<QueryExplainInformation>> should,
    Optional<List<QueryExplainInformation>> filter,
    Optional<Integer> minimumShouldMatch)
    implements LuceneQuerySpecification {
  static class Fields {
    static final Field.Optional<List<QueryExplainInformation>> MUST =
        Field.builder("must")
            .classField(QueryExplainInformation::fromBson)
            .disallowUnknownFields()
            .asList()
            .optional()
            .noDefault();

    static final Field.Optional<List<QueryExplainInformation>> MUST_NOT =
        Field.builder("mustNot")
            .classField(QueryExplainInformation::fromBson)
            .disallowUnknownFields()
            .asList()
            .optional()
            .noDefault();

    static final Field.Optional<List<QueryExplainInformation>> SHOULD =
        Field.builder("should")
            .classField(QueryExplainInformation::fromBson)
            .disallowUnknownFields()
            .asList()
            .optional()
            .noDefault();

    static final Field.Optional<List<QueryExplainInformation>> FILTER =
        Field.builder("filter")
            .classField(QueryExplainInformation::fromBson)
            .disallowUnknownFields()
            .asList()
            .optional()
            .noDefault();

    static final Field.Optional<Integer> MINIMUM_SHOULD_MATCH =
        Field.builder("minimumShouldMatch").intField().optional().noDefault();
  }

  static BooleanQuerySpec fromBson(DocumentParser parser) throws BsonParseException {
    return new BooleanQuerySpec(
        parser.getField(Fields.MUST).unwrap(),
        parser.getField(Fields.MUST_NOT).unwrap(),
        parser.getField(Fields.SHOULD).unwrap(),
        parser.getField(Fields.FILTER).unwrap(),
        parser.getField(Fields.MINIMUM_SHOULD_MATCH).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.MUST, this.must)
        .field(Fields.MUST_NOT, this.mustNot)
        .field(Fields.SHOULD, this.should)
        .field(Fields.FILTER, this.filter)
        .field(Fields.MINIMUM_SHOULD_MATCH, this.minimumShouldMatch)
        .build();
  }

  @Override
  public Type getType() {
    return Type.BOOLEAN_QUERY;
  }

  // returns a version of the BooleanQuerySpec where the child QuerySpecs are sorted by a
  // comparator. Useful for testing explain output without worrying about the determinism of
  // the order of Lucene disjunction/conjunctions.
  BooleanQuerySpec sorted(Comparator<QueryExplainInformation> comparator) {
    return new BooleanQuerySpec(
        this.must.map(
            clauseList ->
                QueryExplainInformation.cloneAndSortChildQuerySpecs(clauseList, comparator)),
        this.mustNot.map(
            clauseList ->
                QueryExplainInformation.cloneAndSortChildQuerySpecs(clauseList, comparator)),
        this.should.map(
            clauseList ->
                QueryExplainInformation.cloneAndSortChildQuerySpecs(clauseList, comparator)),
        this.filter.map(
            clauseList ->
                QueryExplainInformation.cloneAndSortChildQuerySpecs(clauseList, comparator)),
        this.minimumShouldMatch);
  }

  @Override
  public boolean equals(
      LuceneQuerySpecification other,
      Equator<QueryExplainInformation> equator,
      Comparator<QueryExplainInformation> comparator) {
    if (other.getType() != Type.BOOLEAN_QUERY) {
      return false;
    }

    BooleanQuerySpec otherBoolean = (BooleanQuerySpec) other;
    return Equality.equals(this.minimumShouldMatch, otherBoolean.minimumShouldMatch)
        && clauseListEquals(this.must, otherBoolean.must, equator, comparator)
        && clauseListEquals(this.mustNot, otherBoolean.mustNot, equator, comparator)
        && clauseListEquals(this.should, otherBoolean.should, equator, comparator)
        && clauseListEquals(this.filter, otherBoolean.filter, equator, comparator);
  }

  static boolean clauseListEquals(
      Optional<List<QueryExplainInformation>> a,
      Optional<List<QueryExplainInformation>> b,
      Equator<QueryExplainInformation> equator,
      Comparator<QueryExplainInformation> comparator) {
    // If either is empty, the other must also be an empty optional or an empty list to be equal.
    // Note that the following if-block depends on this check.
    if (a.isEmpty()) {
      return b.isEmpty() || b.get().isEmpty();
    }

    if (b.isEmpty()) {
      // Depends on a being present - if a.isEmpty() is true, this method will return from the
      // preceding if block.
      return a.get().isEmpty();
    }

    List<QueryExplainInformation> first = a.get();
    List<QueryExplainInformation> second = b.get();
    if (first.size() != second.size()) {
      return false;
    }

    // Copy the input arrays; equals() is expected to be thread safe and have no side effects.
    var firstCopy = new ArrayList<>(first);
    firstCopy.sort(comparator);
    var secondCopy = new ArrayList<>(second);
    secondCopy.sort(comparator);
    for (int i = 0; i != firstCopy.size(); i++) {
      if (!equator.equate(firstCopy.get(i), secondCopy.get(i))) {
        return false;
      }
    }

    return true;
  }
}
