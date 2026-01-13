package com.xgen.mongot.index.query.operators;

import com.xgen.mongot.index.query.scores.Score;
import com.xgen.mongot.util.Optionals;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.ParsedField;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.bson.BsonValue;

public record CompoundOperator(
    Score score,
    CompoundClause filter,
    CompoundClause must,
    CompoundClause mustNot,
    CompoundClause should,
    int minimumShouldMatch,
    Optional<List<String>> doesNotAffect)
    implements Operator {

  public static class Fields {
    static Field.Optional<List<Operator>> clauseField(String name) {
      return Field.builder(name)
          .classField(Operator::exactlyOneFromBson)
          .disallowUnknownFields()
          .asSingleValueOrList()
          .optional()
          .noDefault();
    }

    public static final Field.Optional<List<Operator>> FILTER = clauseField("filter");
    public static final Field.Optional<List<Operator>> MUST = clauseField("must");
    public static final Field.Optional<List<Operator>> MUST_NOT = clauseField("mustNot");
    public static final Field.Optional<List<Operator>> SHOULD = clauseField("should");

    public static final Field.Optional<List<String>> DOES_NOT_AFFECT =
        Field.builder("doesNotAffect")
            .singleValueOrListOf(
                com.xgen.mongot.util.bson.parser.Value.builder()
                    .stringValue()
                    .mustNotBeEmpty()
                    .required())
            .optional()
            .noDefault();

    public static final int DEFAULT_MINIMUM_SHOULD_MATCH = 0;

    public static final Field.WithDefault<Integer> MINIMUM_SHOULD_MATCH =
        Field.builder("minimumShouldMatch")
            .intField()
            .mustBeNonNegative()
            .optional()
            .withDefault(DEFAULT_MINIMUM_SHOULD_MATCH);

    public static final Field.WithDefault<Score> SCORE =
        Field.builder("score")
            .classField(Score::fromBsonAllowDismax)
            .disallowUnknownFields()
            .optional()
            .withDefault(Score.defaultScore());
  }

  public CompoundOperator(
      Score score,
      Optional<CompoundClause> filter,
      Optional<CompoundClause> must,
      Optional<CompoundClause> mustNot,
      Optional<CompoundClause> should,
      int minimumShouldMatch,
      Optional<List<String>> doesNotAffect) {
    this(
        score,
        filter.orElse(CompoundClause.EMPTY),
        must.orElse(CompoundClause.EMPTY),
        mustNot.orElse(CompoundClause.EMPTY),
        should.orElse(CompoundClause.EMPTY),
        minimumShouldMatch,
        doesNotAffect);
  }

  public static CompoundOperator fromBson(DocumentParser parser) throws BsonParseException {
    var filterField = parser.getField(Fields.FILTER);
    var mustField = parser.getField(Fields.MUST);
    var mustNotField = parser.getField(Fields.MUST_NOT);
    var shouldField = parser.getField(Fields.SHOULD);
    parser.getGroup().atLeastOneOf(filterField, mustField, mustNotField, shouldField);

    var score = parser.getField(Fields.SCORE).unwrap();
    var filter = filterField.unwrap();
    var must = mustField.unwrap();
    var mustNot = mustNotField.unwrap();
    var should = shouldField.unwrap();
    validateAtLeastOneClause(parser.getContext(), filter, must, mustNot, should);
    validateDismax(parser.getContext(), score, filter, must, mustNot, should);

    var minimumShouldMatchField = parser.getField(Fields.MINIMUM_SHOULD_MATCH);
    validateMinimumShouldMatch(parser.getContext(), should, minimumShouldMatchField);

    return new CompoundOperator(
        score,
        filter.map(CompoundClause::new),
        must.map(CompoundClause::new),
        mustNot.map(CompoundClause::new),
        should.map(CompoundClause::new),
        minimumShouldMatchField.unwrap(),
        parser.getField(Fields.DOES_NOT_AFFECT).unwrap());
  }

  @Override
  public BsonValue operatorToBson() {
    return Operators.documentBuilder(score())
        .field(Fields.FILTER, Optional.of(new ArrayList<>(this.filter.operators())))
        .field(Fields.MUST, Optional.of(new ArrayList<>(this.must.operators())))
        .field(Fields.MUST_NOT, Optional.of(new ArrayList<>(this.mustNot.operators())))
        .field(Fields.SHOULD, Optional.of(new ArrayList<>(this.should.operators())))
        .field(Fields.MINIMUM_SHOULD_MATCH, this.minimumShouldMatch)
        .field(Fields.DOES_NOT_AFFECT, this.doesNotAffect)
        .build();
  }

  @Override
  public Type getType() {
    return Type.COMPOUND;
  }

  public Stream<Operator> getOperators() {
    return Stream.of(
            this.filter.operators(),
            this.must.operators(),
            this.mustNot.operators(),
            this.should.operators())
        .flatMap(List::stream);
  }

  private static void validateAtLeastOneClause(
      BsonParseContext context,
      Optional<List<Operator>> filter,
      Optional<List<Operator>> must,
      Optional<List<Operator>> mustNot,
      Optional<List<Operator>> should)
      throws BsonParseException {
    var numClauses =
        Optionals.present(Stream.of(filter, must, mustNot, should))
            .mapToLong(Collection::size)
            .sum();

    if (numClauses == 0) {
      context.handleSemanticError("must have at least one clause");
    }
  }

  private static void validateDismax(
      BsonParseContext context,
      Score score,
      Optional<List<Operator>> filter,
      Optional<List<Operator>> must,
      Optional<List<Operator>> mustNot,
      Optional<List<Operator>> should)
      throws BsonParseException {
    if (score.getType() != Score.Type.DISMAX) {
      return;
    }

    if (should.isEmpty()) {
      context.handleSemanticError("should clause must not be empty when using dismax score");
    }

    if (must.isPresent() || mustNot.isPresent() || filter.isPresent()) {
      context.handleSemanticError("cannot use dismax when must, mustNot, or filter is present");
    }
  }

  private static void validateMinimumShouldMatch(
      BsonParseContext context,
      Optional<List<Operator>> should,
      ParsedField.WithDefault<Integer> minimumShouldMatchField)
      throws BsonParseException {
    if (minimumShouldMatchField.isEmpty()) {
      return;
    }

    if (should.isEmpty()) {
      context.handleSemanticError("should clause must not be empty when using minimumShouldMatch");
    }

    if (minimumShouldMatchField.unwrap() > should.get().size()) {
      context.handleSemanticError(
          "minimumShouldMatch cannot be greater than number of should clauses");
    }
  }
}
