package com.xgen.mongot.index.analyzer.custom;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Var;
import com.xgen.mongot.index.analyzer.attributes.TokenStreamTransformationStage;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.List;
import java.util.Objects;
import org.apache.lucene.analysis.miscellaneous.WordDelimiterGraphFilter;
import org.bson.BsonDocument;

public class WordDelimiterGraphTokenFilterDefinition extends TokenFilterDefinition
    implements TokenStreamTransformationStage.AnyToGraph {

  public static class ProtectedWords implements DocumentEncodable {
    static class Fields {
      static final Field.Required<List<String>> WORDS =
          Field.builder("words").stringField().mustNotBeBlank().asList().required();
      static final Field.WithDefault<Boolean> IGNORE_CASE =
          Field.builder("ignoreCase").booleanField().optional().withDefault(true);
    }

    public final ImmutableList<String> words;

    public final boolean ignoreCase;

    public ProtectedWords(List<String> words, boolean ignoreCase) {
      this.words = ImmutableList.copyOf(words);
      this.ignoreCase = ignoreCase;
    }

    static ProtectedWords createDefault() {
      return new ProtectedWords(List.of(), true);
    }

    static ProtectedWords fromBson(DocumentParser parser) throws BsonParseException {
      return new ProtectedWords(
          parser.getField(Fields.WORDS).unwrap(), parser.getField(Fields.IGNORE_CASE).unwrap());
    }

    @Override
    public BsonDocument toBson() {
      return BsonDocumentBuilder.builder()
          .field(Fields.WORDS, this.words)
          .field(Fields.IGNORE_CASE, this.ignoreCase)
          .build();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ProtectedWords that = (ProtectedWords) o;
      return this.ignoreCase == that.ignoreCase && this.words.equals(that.words);
    }

    @Override
    public int hashCode() {
      return Objects.hash(this.words, this.ignoreCase);
    }
  }

  public static class DelimiterOptions implements DocumentEncodable {
    static class Fields {
      static final Field.WithDefault<Boolean> GENERATE_WORD_PARTS =
          Field.builder("generateWordParts").booleanField().optional().withDefault(true);
      static final Field.WithDefault<Boolean> GENERATE_NUMBER_PARTS =
          Field.builder("generateNumberParts").booleanField().optional().withDefault(true);
      static final Field.WithDefault<Boolean> CONCATENATE_WORDS =
          Field.builder("concatenateWords").booleanField().optional().withDefault(false);
      static final Field.WithDefault<Boolean> CONCATENATE_NUMBERS =
          Field.builder("concatenateNumbers").booleanField().optional().withDefault(false);
      static final Field.WithDefault<Boolean> CONCATENATE_ALL =
          Field.builder("concatenateAll").booleanField().optional().withDefault(false);
      static final Field.WithDefault<Boolean> PRESERVE_ORIGINAL =
          Field.builder("preserveOriginal").booleanField().optional().withDefault(false);
      static final Field.WithDefault<Boolean> SPLIT_ON_CASE_CHANGE =
          Field.builder("splitOnCaseChange").booleanField().optional().withDefault(true);
      static final Field.WithDefault<Boolean> SPLIT_ON_NUMERICS =
          Field.builder("splitOnNumerics").booleanField().optional().withDefault(true);
      static final Field.WithDefault<Boolean> STEM_ENGLISH_POSSESSIVE =
          Field.builder("stemEnglishPossessive").booleanField().optional().withDefault(true);
      static final Field.WithDefault<Boolean> IGNORE_KEYWORDS =
          Field.builder("ignoreKeywords").booleanField().optional().withDefault(false);
    }

    final boolean generateWordParts;
    final boolean generateNumberParts;
    final boolean concatenateWords;
    final boolean concatenateNumbers;
    final boolean concatenateAll;
    final boolean preserveOriginal;
    final boolean splitOnCaseChange;
    final boolean splitOnNumerics;
    final boolean stemEnglishPossessive;
    final boolean ignoreKeywords;

    public DelimiterOptions(
        boolean generateWordParts,
        boolean generateNumberParts,
        boolean concatenateWords,
        boolean concatenateNumbers,
        boolean concatenateAll,
        boolean preserveOriginal,
        boolean splitOnCaseChange,
        boolean splitOnNumerics,
        boolean stemEnglishPossessive,
        boolean ignoreKeywords) {
      this.generateWordParts = generateWordParts;
      this.generateNumberParts = generateNumberParts;
      this.concatenateWords = concatenateWords;
      this.concatenateNumbers = concatenateNumbers;
      this.concatenateAll = concatenateAll;
      this.preserveOriginal = preserveOriginal;
      this.splitOnCaseChange = splitOnCaseChange;
      this.splitOnNumerics = splitOnNumerics;
      this.stemEnglishPossessive = stemEnglishPossessive;
      this.ignoreKeywords = ignoreKeywords;
    }

    static DelimiterOptions createDefault() {
      return new DelimiterOptions(
          Fields.GENERATE_WORD_PARTS.getDefaultValue(),
          Fields.GENERATE_NUMBER_PARTS.getDefaultValue(),
          Fields.CONCATENATE_WORDS.getDefaultValue(),
          Fields.CONCATENATE_NUMBERS.getDefaultValue(),
          Fields.CONCATENATE_ALL.getDefaultValue(),
          Fields.PRESERVE_ORIGINAL.getDefaultValue(),
          Fields.SPLIT_ON_CASE_CHANGE.getDefaultValue(),
          Fields.SPLIT_ON_NUMERICS.getDefaultValue(),
          Fields.STEM_ENGLISH_POSSESSIVE.getDefaultValue(),
          Fields.IGNORE_KEYWORDS.getDefaultValue());
    }

    static DelimiterOptions fromBson(DocumentParser parser) throws BsonParseException {
      return new DelimiterOptions(
          parser.getField(Fields.GENERATE_WORD_PARTS).unwrap(),
          parser.getField(Fields.GENERATE_NUMBER_PARTS).unwrap(),
          parser.getField(Fields.CONCATENATE_WORDS).unwrap(),
          parser.getField(Fields.CONCATENATE_NUMBERS).unwrap(),
          parser.getField(Fields.CONCATENATE_ALL).unwrap(),
          parser.getField(Fields.PRESERVE_ORIGINAL).unwrap(),
          parser.getField(Fields.SPLIT_ON_CASE_CHANGE).unwrap(),
          parser.getField(Fields.SPLIT_ON_NUMERICS).unwrap(),
          parser.getField(Fields.STEM_ENGLISH_POSSESSIVE).unwrap(),
          parser.getField(Fields.IGNORE_KEYWORDS).unwrap());
    }

    @Override
    public BsonDocument toBson() {
      return BsonDocumentBuilder.builder()
          .field(Fields.GENERATE_WORD_PARTS, this.generateWordParts)
          .field(Fields.GENERATE_NUMBER_PARTS, this.generateNumberParts)
          .field(Fields.CONCATENATE_WORDS, this.concatenateWords)
          .field(Fields.CONCATENATE_NUMBERS, this.concatenateNumbers)
          .field(Fields.CONCATENATE_ALL, this.concatenateAll)
          .field(Fields.PRESERVE_ORIGINAL, this.preserveOriginal)
          .field(Fields.SPLIT_ON_CASE_CHANGE, this.splitOnCaseChange)
          .field(Fields.SPLIT_ON_NUMERICS, this.splitOnNumerics)
          .field(Fields.STEM_ENGLISH_POSSESSIVE, this.stemEnglishPossessive)
          .field(Fields.IGNORE_KEYWORDS, this.ignoreKeywords)
          .build();
    }

    @SuppressWarnings("checkstyle:MissingJavadocMethod")
    public int getConfigurationFlags() {
      @Var int flags = 0;

      if (this.generateWordParts) {
        flags |= WordDelimiterGraphFilter.GENERATE_WORD_PARTS;
      }
      if (this.generateNumberParts) {
        flags |= WordDelimiterGraphFilter.GENERATE_NUMBER_PARTS;
      }
      if (this.concatenateWords) {
        flags |= WordDelimiterGraphFilter.CATENATE_WORDS;
      }
      if (this.concatenateNumbers) {
        flags |= WordDelimiterGraphFilter.CATENATE_NUMBERS;
      }
      if (this.concatenateAll) {
        flags |= WordDelimiterGraphFilter.CATENATE_ALL;
      }
      if (this.preserveOriginal) {
        flags |= WordDelimiterGraphFilter.PRESERVE_ORIGINAL;
      }
      if (this.splitOnCaseChange) {
        flags |= WordDelimiterGraphFilter.SPLIT_ON_CASE_CHANGE;
      }
      if (this.splitOnNumerics) {
        flags |= WordDelimiterGraphFilter.SPLIT_ON_NUMERICS;
      }
      if (this.stemEnglishPossessive) {
        flags |= WordDelimiterGraphFilter.STEM_ENGLISH_POSSESSIVE;
      }
      if (this.ignoreKeywords) {
        flags |= WordDelimiterGraphFilter.IGNORE_KEYWORDS;
      }

      return flags;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      DelimiterOptions that = (DelimiterOptions) o;
      return this.generateWordParts == that.generateWordParts
          && this.generateNumberParts == that.generateNumberParts
          && this.concatenateWords == that.concatenateWords
          && this.concatenateNumbers == that.concatenateNumbers
          && this.concatenateAll == that.concatenateAll
          && this.preserveOriginal == that.preserveOriginal
          && this.splitOnCaseChange == that.splitOnCaseChange
          && this.splitOnNumerics == that.splitOnNumerics
          && this.stemEnglishPossessive == that.stemEnglishPossessive
          && this.ignoreKeywords == that.ignoreKeywords;
    }

    @Override
    public int hashCode() {
      return Objects.hash(
          this.generateWordParts,
          this.generateNumberParts,
          this.concatenateWords,
          this.concatenateNumbers,
          this.concatenateAll,
          this.preserveOriginal,
          this.splitOnCaseChange,
          this.splitOnNumerics,
          this.stemEnglishPossessive,
          this.ignoreKeywords);
    }
  }

  static class Fields {
    static final Field.WithDefault<ProtectedWords> PROTECTED_WORDS =
        Field.builder("protectedWords")
            .classField(ProtectedWords::fromBson)
            .disallowUnknownFields()
            .optional()
            .withDefault(ProtectedWords.createDefault());

    static final Field.WithDefault<DelimiterOptions> DELIMITER_OPTIONS =
        Field.builder("delimiterOptions")
            .classField(DelimiterOptions::fromBson)
            .disallowUnknownFields()
            .optional()
            .withDefault(DelimiterOptions.createDefault());
  }

  public final ProtectedWords protectedWords;
  public final DelimiterOptions delimiterOptions;

  public WordDelimiterGraphTokenFilterDefinition(
      ProtectedWords protectedWords, DelimiterOptions delimiterOptions) {
    this.protectedWords = protectedWords;
    this.delimiterOptions = delimiterOptions;
  }

  @Override
  BsonDocument tokenFilterToBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.PROTECTED_WORDS, this.protectedWords)
        .field(Fields.DELIMITER_OPTIONS, this.delimiterOptions)
        .build();
  }

  /** Deserialize a WordDelimiterGraphTokenFilterDefinition. */
  public static WordDelimiterGraphTokenFilterDefinition fromBson(DocumentParser parser)
      throws BsonParseException {
    return new WordDelimiterGraphTokenFilterDefinition(
        parser.getField(Fields.PROTECTED_WORDS).unwrap(),
        parser.getField(Fields.DELIMITER_OPTIONS).unwrap());
  }

  @Override
  public Type getType() {
    return Type.WORD_DELIMITER_GRAPH;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    WordDelimiterGraphTokenFilterDefinition that = (WordDelimiterGraphTokenFilterDefinition) o;
    return this.protectedWords.equals(that.protectedWords)
        && this.delimiterOptions.equals(that.delimiterOptions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.protectedWords, this.delimiterOptions);
  }
}
