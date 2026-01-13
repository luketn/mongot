package com.xgen.mongot.index.analyzer.definition;

import com.xgen.mongot.index.analyzer.attributes.TokenStreamType;
import com.xgen.mongot.index.analyzer.attributes.TokenStreamTypeAware;
import com.xgen.mongot.index.analyzer.custom.CharFilterDefinition;
import com.xgen.mongot.index.analyzer.custom.TokenFilterDefinition;
import com.xgen.mongot.index.analyzer.custom.TokenizerDefinition;
import com.xgen.mongot.index.lucene.analyzer.CustomAnalyzerSpecification;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import org.bson.BsonDocument;

public record CustomAnalyzerDefinition(
    String name,
    Optional<List<CharFilterDefinition>> charFilters,
    TokenizerDefinition tokenizer,
    Optional<List<TokenFilterDefinition>> tokenFilters)
    implements AnalyzerDefinition,
        DocumentEncodable,
        CustomAnalyzerSpecification,
        TokenStreamTypeAware {

  static class Fields {
    static final Field.Required<String> NAME =
        Field.builder("name")
            .stringField()
            .mustNotBeginWith("lucene.")
            .mustNotBeginWith("builtin.")
            .mustNotBeginWith("mongodb.")
            .required();

    static final Field.Optional<List<CharFilterDefinition>> CHAR_FILTERS =
        Field.builder("charFilters")
            .classField(CharFilterDefinition::fromBson)
            .disallowUnknownFields()
            .asList()
            .optional()
            .noDefault();

    static final Field.Required<TokenizerDefinition> TOKENIZER =
        Field.builder("tokenizer")
            .classField(TokenizerDefinition::fromBson)
            .disallowUnknownFields()
            .required();

    static final Field.Optional<List<TokenFilterDefinition>> TOKEN_FILTERS =
        Field.builder("tokenFilters")
            .classField(TokenFilterDefinition::fromBson)
            .disallowUnknownFields()
            .asList()
            .optional()
            .noDefault();
  }

  @Override
  public TokenStreamType getTokenStreamType() {
    // Start with output type of the tokenizer and apply the output type of each token filter
    // until we know the final output type.
    return this.tokenFilters.stream()
        .flatMap(Collection::stream)
        .reduce(
            this.tokenizer.getTokenStreamType(),
            (inputType, tokenFilter) -> {
              TokenStreamType resultingTokenStreamType = tokenFilter.outputTypeGiven(inputType);
              Check.argNotNull(resultingTokenStreamType, "token stream type returned is null");
              return resultingTokenStreamType;
            },
            (inputType, outputType) -> outputType);
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.NAME, this.name)
        .field(Fields.CHAR_FILTERS, this.charFilters)
        .field(Fields.TOKENIZER, this.tokenizer)
        .field(Fields.TOKEN_FILTERS, this.tokenFilters)
        .build();
  }

  /** Deserialize from a BSONDocument directly. */
  public static CustomAnalyzerDefinition fromBson(BsonDocument document) throws BsonParseException {
    try (var parser = BsonDocumentParser.fromRoot(document).build()) {
      return fromBson(parser);
    }
  }

  /** Deserialize a BSON document into a new CustomAnalyzerDefinition. */
  public static CustomAnalyzerDefinition fromBson(DocumentParser parser) throws BsonParseException {
    return new CustomAnalyzerDefinition(
        parser.getField(Fields.NAME).unwrap(),
        parser.getField(Fields.CHAR_FILTERS).unwrap(),
        parser.getField(Fields.TOKENIZER).unwrap(),
        parser.getField(Fields.TOKEN_FILTERS).unwrap());
  }
}
