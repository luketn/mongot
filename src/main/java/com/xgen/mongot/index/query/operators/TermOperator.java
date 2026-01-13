package com.xgen.mongot.index.query.operators;

import com.xgen.mongot.index.path.string.StringPath;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.List;
import java.util.stream.Stream;
import org.apache.commons.lang3.Range;
import org.bson.BsonDocument;

public sealed interface TermOperator extends Operator
    permits TermBaseOperator,
        TermFuzzyOperator,
        TermPrefixOperator,
        TermRegexOperator,
        TermWildcardOperator {

  class Fields {
    static final Field.Optional<FuzzyOption> FUZZY =
        Field.builder("fuzzy")
            .classField(FuzzyOption::fromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();

    public static final Field.WithDefault<Boolean> PREFIX =
        Field.builder("prefix").booleanField().optional().withDefault(false);

    public static final Field.WithDefault<Boolean> REGEX =
        Field.builder("regex").booleanField().optional().withDefault(false);

    public static final Field.WithDefault<Boolean> WILDCARD =
        Field.builder("wildcard").booleanField().optional().withDefault(false);
  }

  String MULTIPLE_OPTIONS_ERROR =
      String.format(
          "only one of [%s] may be present",
          String.join(
              ", ",
              Fields.FUZZY.getName(),
              Fields.PREFIX.getName(),
              Fields.REGEX.getName(),
              Fields.WILDCARD.getName()));

  static TermOperator fromBson(DocumentParser parser) throws BsonParseException {
    var score = Operators.parseScore(parser);
    var path = Operators.parseStringPath(parser);
    var query = Operators.parseQuery(parser);
    var optionalFuzzy = parser.getField(Fields.FUZZY).unwrap();

    var fuzzyPresent = optionalFuzzy.isPresent();
    var prefix = parser.getField(Fields.PREFIX).unwrap();
    var regex = parser.getField(Fields.REGEX).unwrap();
    var wildcard = parser.getField(Fields.WILDCARD).unwrap();

    // Cannot use parser.getGroup().atMostOneOf() since we want to allow
    // { ..., prefix: true, regex: false }; the presence of the field doesn't mean the mutual
    // exclusion is violated.
    var multipleOptionsSet =
        Stream.of(fuzzyPresent, prefix, regex, wildcard).filter(Boolean::booleanValue).count() > 1;
    if (multipleOptionsSet) {
      parser.getContext().handleSemanticError(MULTIPLE_OPTIONS_ERROR);
    }

    if (fuzzyPresent) {
      var fuzzy = optionalFuzzy.get();
      return new TermFuzzyOperator(score, path, query, fuzzy);
    }

    if (prefix) {
      return new TermPrefixOperator(score, path, query);
    }

    if (regex) {
      return new TermRegexOperator(score, path, query);
    }

    if (wildcard) {
      return new TermWildcardOperator(score, path, query);
    }

    return new TermBaseOperator(score, path, query);
  }

  List<StringPath> paths();

  List<String> query();

  record FuzzyOption(int maxEdits, int prefixLength, int maxExpansions)
      implements DocumentEncodable {

    public static class Fields {
      public static final Field.WithDefault<Integer> MAX_EDITS =
          Field.builder("maxEdits")
              .intField()
              /*
               * Per Lucene Docs, this query will match terms up to 2 edits. Higher distances
               * (especially with transpositions enabled), are generally not useful and will match a
               * significant amount of the term dictionary.
               * https://lucene.apache.org/core/6_5_0/core/constant-values.html#org.apache.lucene.util.automaton.LevenshteinAutomata.MAXIMUM_SUPPORTED_DISTANCE
               */
              .mustBeWithinBounds(Range.of(0, 2))
              .optional()
              .withDefault(2);

      public static final Field.WithDefault<Integer> MAX_EXPANSIONS =
          Field.builder("maxExpansions").intField().mustBePositive().optional().withDefault(256);

      public static final Field.WithDefault<Integer> PREFIX_LENGTH =
          Field.builder("prefixLength").intField().mustBeNonNegative().optional().withDefault(0);
    }

    public static FuzzyOption fromBson(DocumentParser parser) throws BsonParseException {
      return new FuzzyOption(
          parser.getField(Fields.MAX_EDITS).unwrap(),
          parser.getField(Fields.PREFIX_LENGTH).unwrap(),
          parser.getField(Fields.MAX_EXPANSIONS).unwrap());
    }

    @Override
    public BsonDocument toBson() {
      return BsonDocumentBuilder.builder()
          .field(Fields.MAX_EDITS, this.maxEdits)
          .field(Fields.PREFIX_LENGTH, this.prefixLength)
          .field(Fields.MAX_EXPANSIONS, this.maxExpansions)
          .build();
    }
  }
}
