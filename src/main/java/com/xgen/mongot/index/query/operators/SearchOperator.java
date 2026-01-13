package com.xgen.mongot.index.query.operators;

import com.xgen.mongot.index.path.string.StringPath;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.List;
import org.apache.commons.lang3.Range;
import org.bson.BsonDocument;

public sealed interface SearchOperator extends Operator
    permits SearchDisjunctionOperator, SearchPhraseOperator, SearchPhrasePrefixOperator {
  class Fields {
    public static final Field.Optional<PhraseOption> PHRASE =
        Field.builder("phrase")
            .classField(PhraseOption::fromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();
  }

  /** Deserializes a SearchOperator from the supplied DocumentParser. */
  static SearchOperator fromBson(DocumentParser parser) throws BsonParseException {
    var baseOperator =
        new SearchDisjunctionOperator(
            Operators.parseScore(parser),
            Operators.parseStringPath(parser),
            Operators.parseQuery(parser));
    var optionalPhraseOptions = parser.getField(Fields.PHRASE).unwrap();

    if (optionalPhraseOptions.isEmpty()) {
      return baseOperator;
    }

    var phraseOptions = optionalPhraseOptions.get();
    if (phraseOptions.prefix()) {
      return new SearchPhrasePrefixOperator(baseOperator, phraseOptions);
    }

    return new SearchPhraseOperator(baseOperator, phraseOptions);
  }

  List<StringPath> paths();

  List<String> query();

  record PhraseOption(int maxExpansions, boolean prefix, int slop) implements DocumentEncodable {

    public static class Fields {
      public static final Field.WithDefault<Integer> MAX_EXPANSIONS =
          Field.builder("maxExpansions")
              .intField()
              .mustBeWithinBounds(Range.of(0, 1000))
              .optional()
              .withDefault(50);

      public static final Field.WithDefault<Boolean> PREFIX =
          Field.builder("prefix").booleanField().optional().withDefault(false);

      public static final Field.WithDefault<Integer> SLOP =
          Field.builder("slop")
              .intField()
              .mustBeWithinBounds(Range.of(0, 100))
              .optional()
              .withDefault(0);
    }

    static PhraseOption fromBson(DocumentParser parser) throws BsonParseException {
      var slop = parser.getField(Fields.SLOP).unwrap();
      var prefix = parser.getField(Fields.PREFIX).unwrap();
      var maxExpansionsField = parser.getField(Fields.MAX_EXPANSIONS);

      if (!prefix && maxExpansionsField.isPresent()) {
        parser
            .getContext()
            .handleSemanticError("maxExpansions can only be present with prefix: true");
      }

      return new PhraseOption(maxExpansionsField.unwrap(), prefix, slop);
    }

    @Override
    public BsonDocument toBson() {
      return BsonDocumentBuilder.builder()
          .field(Fields.MAX_EXPANSIONS, this.maxExpansions)
          .field(Fields.PREFIX, this.prefix)
          .field(Fields.SLOP, this.slop)
          .build();
    }
  }
}
