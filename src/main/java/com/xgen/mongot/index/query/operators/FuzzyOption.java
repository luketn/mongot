package com.xgen.mongot.index.query.operators;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Optional;
import org.apache.commons.lang3.Range;
import org.bson.BsonDocument;

public record FuzzyOption(int maxEdits, int prefixLength, int maxExpansions)
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
            .validate(
                i -> i == 1 || i == 2 ? Optional.empty() : Optional.of("must be either 1 or 2"))
            .optional()
            .withDefault(2);

    public static final Field.WithDefault<Integer> MAX_EXPANSIONS =
        Field.builder("maxExpansions")
            .intField()
            /*
             * Note that the cap on max expansions is applied on a per token basis, so the load
             * on memory can potentially be: UPPER_BOUND * |tokens| * |paths|
             */
            .mustBeWithinBounds(Range.of(1, 1000))
            .optional()
            .withDefault(50);

    public static final Field.WithDefault<Integer> PREFIX_LENGTH =
        Field.builder("prefixLength").intField().mustBeNonNegative().optional().withDefault(0);
  }

  static FuzzyOption fromBson(DocumentParser parser) throws BsonParseException {
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
