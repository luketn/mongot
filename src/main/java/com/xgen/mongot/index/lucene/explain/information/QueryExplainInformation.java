package com.xgen.mongot.index.lucene.explain.information;

import com.google.errorprone.annotations.CheckReturnValue;
import com.xgen.mongot.index.lucene.explain.timing.ExplainTimingBreakdown;
import com.xgen.mongot.util.Equality;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.FieldPathField;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import org.apache.commons.collections4.Equator;
import org.bson.BsonDocument;

public record QueryExplainInformation(
    Optional<FieldPath> path,
    LuceneQuerySpecification.Type type,
    Optional<String> analyzer,
    LuceneQuerySpecification args,
    Optional<ExplainTimingBreakdown> stats)
    implements DocumentEncodable {
  static class Fields {
    static final Field.Optional<FieldPath> PATH =
        Field.builder("path")
            .classField(FieldPathField::parse, FieldPathField::encode)
            .optional()
            .noDefault();

    static final Field.Required<LuceneQuerySpecification.Type> TYPE =
        Field.builder("type")
            .enumField(LuceneQuerySpecification.Type.class)
            .asUpperCamelCase()
            .required();

    static final Field.Optional<String> ANALYZER =
        Field.builder("analyzer").stringField().optional().noDefault();

    static final Field.Optional<ExplainTimingBreakdown> STATS =
        Field.builder("stats")
            .classField(ExplainTimingBreakdown::fromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();
  }

  // sort child nodes of dismax/boolean Lucene queries. Useful for testing
  // explain output without worrying about the determinism of the order of Lucene
  // disjunction/conjunctions. If not dismax/boolean, this is a no-op. This is necessary to support
  // testing on nested Boolean/Dismax operations, where the second-layer query that isn't sorted
  // prior to comparison would cause the sort to be non-deterministic for equivalent queries
  // Ex:
  // (BooleanQuery [TermQueryA, BoostQueryB]) and (BooleanQuery [BoostQueryB, TermQueryA]) as
  // clauses in a boolean query would get sorted differently in a normal sort, even though they're
  // functionally the same query, so we need to access the child query and sort those first
  @CheckReturnValue
  public QueryExplainInformation sortedArgs(Comparator<QueryExplainInformation> comparator) {
    return switch (this.type()) {
      case BOOLEAN_QUERY ->
          new QueryExplainInformation(
              this.path,
              LuceneQuerySpecification.Type.BOOLEAN_QUERY,
              this.analyzer,
              ((BooleanQuerySpec) this.args).sorted(comparator),
              this.stats);
      case DISJUNCTION_MAX_QUERY ->
          new QueryExplainInformation(
              this.path,
              LuceneQuerySpecification.Type.DISJUNCTION_MAX_QUERY,
              this.analyzer,
              ((DisjunctionMaxQuerySpec) this.args).sorted(comparator),
              this.stats);
      default -> this;
    };
  }

  public static QueryExplainInformation fromBson(DocumentParser parser) throws BsonParseException {
    LuceneQuerySpecification.Type type = parser.getField(Fields.TYPE).unwrap();
    Field.Required<LuceneQuerySpecification> args = LuceneQuerySpecification.argsFieldForType(type);

    return new QueryExplainInformation(
        parser.getField(Fields.PATH).unwrap(),
        type,
        parser.getField(Fields.ANALYZER).unwrap(),
        parser.getField(args).unwrap(),
        parser.getField(Fields.STATS).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    Field.Required<LuceneQuerySpecification> args =
        LuceneQuerySpecification.argsFieldForType(this.type);

    return BsonDocumentBuilder.builder()
        .field(Fields.PATH, this.path)
        .field(Fields.TYPE, this.type)
        .field(Fields.ANALYZER, this.analyzer)
        .field(args, this.args)
        .field(Fields.STATS, this.stats)
        .build();
  }

  /** Equality check, ignoring timing statistics. */
  public boolean equals(
      QueryExplainInformation other, Equator<LuceneQuerySpecification> queryEquator) {
    return equals(other, queryEquator, Equality.alwaysEqualEquator());
  }

  /**
   * Tests equality of two LuceneExplainInformation objects.
   *
   * <p>The motivation behind making the caller pass in Equators for some classes is to be super
   * explicit about what is being tested here. This equals method is "complete" in the sense that
   * they test all the properties of a LuceneExplainInformation - though if the caller wants to
   * compare the Operator/LuceneQuerySpecification/ExplainTimingBreakdown, it has to provide its own
   * means of comparison.
   */
  public boolean equals(
      QueryExplainInformation other,
      Equator<LuceneQuerySpecification> queryEquator,
      Equator<ExplainTimingBreakdown> timingEquator) {
    return this.type == other.type
        && Equality.equals(this.path, other.path)
        && Equality.equals(this.analyzer, other.analyzer)
        && queryEquator.equate(this.args, other.args)
        && Equality.equals(this.stats, other.stats, timingEquator);
  }

  static List<QueryExplainInformation> cloneAndSortChildQuerySpecs(
      List<QueryExplainInformation> clauseList, Comparator<QueryExplainInformation> comparator) {
    List<QueryExplainInformation> clonedList = new ArrayList<>();
    for (var clause : clauseList) {
      clonedList.add(clause.sortedArgs(comparator));
    }
    clonedList.sort(comparator);
    return clonedList;
  }
}
