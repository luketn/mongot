package com.xgen.mongot.index.definition;

import com.xgen.mongot.index.analyzer.definition.StockAnalyzerNames;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.ParsedField;
import java.util.Objects;
import java.util.Optional;
import org.bson.BsonDocument;

public final class AutocompleteFieldDefinition implements FieldTypeDefinition {

  private static final StockAnalyzerNames DEFAULT_BASE_ANALYZER =
      StockAnalyzerNames.LUCENE_STANDARD;

  public static class Fields {
    public static final Field.WithDefault<Integer> MIN_GRAMS =
        Field.builder("minGrams").intField().mustBePositive().optional().withDefault(2);

    public static final Field.WithDefault<Integer> MAX_GRAMS =
        Field.builder("maxGrams").intField().mustBePositive().optional().withDefault(15);

    public static final Field.WithDefault<Boolean> FOLD_DIACRITICS =
        Field.builder("foldDiacritics").booleanField().optional().withDefault(true);

    public static final Field.WithDefault<TokenizationStrategy> TOKENIZATION_STRATEGY =
        Field.builder("tokenization")
            .enumField(TokenizationStrategy.class)
            .asCamelCase()
            .optional()
            .withDefault(TokenizationStrategy.EDGE_GRAM);

    static final Field.Optional<String> ANALYZER =
        Field.builder("analyzer").stringField().optional().noDefault();

    static final Field.Optional<SimilarityDefinition> SIMILARITY =
        Field.builder("similarity")
            .classField(SimilarityDefinition::fromBson, SimilarityDefinition::toBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();
  }

  static class ErrorMessages {
    static final String MIN_GRAMS_TOO_BIG = "minGrams cannot be greater than maxGrams";
    static final String MIN_GRAMS_BIGGER_THAN_DEFAULT_MAX_GRAMS =
        String.format(
            "minGrams must be less than or equal to the default maxGrams value of %s",
            Fields.MAX_GRAMS.getDefaultValue());
    static final String MAX_GRAMS_SMALLER_THAN_DEFAULT_MIN_GRAMS =
        String.format(
            "maxGrams must be greater than or equal to the default minGrams value of %s",
            Fields.MIN_GRAMS.getDefaultValue());
  }

  public enum TokenizationStrategy {
    EDGE_GRAM,
    N_GRAM,
    RIGHT_EDGE_GRAM
  }

  private final int minGrams;
  private final int maxGrams;
  private final boolean foldDiacritics;
  private final TokenizationStrategy tokenization;
  private final Optional<String> analyzer;
  private final Optional<SimilarityDefinition> similarity;

  public AutocompleteFieldDefinition(
      int minGrams,
      int maxGrams,
      boolean foldDiacritics,
      TokenizationStrategy tokenization,
      Optional<String> analyzer,
      Optional<SimilarityDefinition> similarity) {
    this.minGrams = minGrams;
    this.maxGrams = maxGrams;
    this.foldDiacritics = foldDiacritics;
    this.tokenization = tokenization;
    this.analyzer = analyzer;
    this.similarity = similarity;
  }

  static AutocompleteFieldDefinition fromBson(DocumentParser parser) throws BsonParseException {
    var minGrams = parser.getField(Fields.MIN_GRAMS);
    var maxGrams = parser.getField(Fields.MAX_GRAMS);
    validateMinAndMaxGrams(parser.getContext(), minGrams, maxGrams);

    return new AutocompleteFieldDefinition(
        minGrams.unwrap(),
        maxGrams.unwrap(),
        parser.getField(Fields.FOLD_DIACRITICS).unwrap(),
        parser.getField(Fields.TOKENIZATION_STRATEGY).unwrap(),
        parser.getField(Fields.ANALYZER).unwrap(),
        parser.getField(Fields.SIMILARITY).unwrap());
  }

  @Override
  public BsonDocument fieldTypeToBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.MIN_GRAMS, this.minGrams)
        .field(Fields.MAX_GRAMS, this.maxGrams)
        .field(Fields.FOLD_DIACRITICS, this.foldDiacritics)
        .field(Fields.TOKENIZATION_STRATEGY, this.tokenization)
        .field(Fields.ANALYZER, this.analyzer)
        .field(Fields.SIMILARITY, this.similarity)
        .build();
  }

  @Override
  public Type getType() {
    return Type.AUTOCOMPLETE;
  }

  public int getMinGrams() {
    return this.minGrams;
  }

  public int getMaxGrams() {
    return this.maxGrams;
  }

  public boolean isFoldDiacritics() {
    return this.foldDiacritics;
  }

  public TokenizationStrategy getTokenization() {
    return this.tokenization;
  }

  public String getAnalyzer() {
    return this.analyzer.orElse(DEFAULT_BASE_ANALYZER.getName());
  }

  public Optional<SimilarityDefinition> getSimilarity() {
    return this.similarity;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }

    if (!(obj instanceof AutocompleteFieldDefinition other)) {
      return false;
    }

    return Objects.equals(this.minGrams, other.minGrams)
        && Objects.equals(this.maxGrams, other.maxGrams)
        && Objects.equals(this.tokenization, other.tokenization)
        && Objects.equals(this.foldDiacritics, other.foldDiacritics)
        && Objects.equals(this.analyzer, other.analyzer)
        && Objects.equals(this.similarity, other.similarity);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        this.tokenization,
        this.foldDiacritics,
        this.minGrams,
        this.maxGrams,
        this.analyzer,
        this.similarity);
  }

  private static void validateMinAndMaxGrams(
      BsonParseContext context,
      ParsedField.WithDefault<Integer> minGrams,
      ParsedField.WithDefault<Integer> maxGrams)
      throws BsonParseException {
    if (minGrams.unwrap() <= maxGrams.unwrap()) {
      return;
    }

    boolean bothExplicit = minGrams.isPresent() && maxGrams.isPresent();
    String message =
        bothExplicit
            ? ErrorMessages.MIN_GRAMS_TOO_BIG
            : minGrams.isPresent()
                ? ErrorMessages.MIN_GRAMS_BIGGER_THAN_DEFAULT_MAX_GRAMS
                : ErrorMessages.MAX_GRAMS_SMALLER_THAN_DEFAULT_MIN_GRAMS;

    context.handleSemanticError(message);
  }
}
