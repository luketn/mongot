package com.xgen.mongot.index.analyzer.definition;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.lucene.analysis.CharArraySet;
import org.bson.BsonDocument;

public final class OverriddenBaseAnalyzerDefinition
    implements AnalyzerDefinition, DocumentEncodable {

  public static class Fields {
    public static final Field.Required<String> NAME =
        Field.builder("name").stringField().mustNotBeEmpty().required();

    static final Field.Required<String> BASE_ANALYZER =
        Field.builder("baseAnalyzer").stringField().mustNotBeEmpty().required();

    public static final Field.WithDefault<Boolean> IGNORE_CASE =
        Field.builder("ignoreCase").booleanField().optional().withDefault(true);

    static final Field.Optional<Integer> MAX_TOKEN_LENGTH =
        Field.builder("maxTokenLength").intField().mustBePositive().optional().noDefault();

    static final Field.Optional<List<String>> STOPWORDS =
        Field.builder("stopwords").stringField().asList().optional().noDefault();

    static final Field.Optional<List<String>> STEM_EXCLUSION_SET =
        Field.builder("stemExclusionSet").stringField().asList().optional().noDefault();
  }

  private final String name;
  private final String baseAnalyzerName;
  private final boolean ignoreCase;
  private final Optional<Integer> maxTokenLength;
  private final Optional<CharArraySet> stopwords;
  private final Optional<CharArraySet> stemExclusionSet;

  public OverriddenBaseAnalyzerDefinition(
      String name,
      String baseAnalyzerName,
      boolean ignoreCase,
      Optional<Integer> maxTokenLength,
      Optional<Set<String>> stopwords,
      Optional<Set<String>> stemExclusionSet) {
    this.name = name;
    this.baseAnalyzerName = baseAnalyzerName;
    this.ignoreCase = ignoreCase;
    this.maxTokenLength = maxTokenLength;
    this.stopwords = stopwords.map(s -> new CharArraySet(s, ignoreCase));
    this.stemExclusionSet = stemExclusionSet.map(s -> new CharArraySet(s, ignoreCase));
  }

  public static OverriddenBaseAnalyzerDefinition stockAnalyzerWithName(String name) {
    return new OverriddenBaseAnalyzerDefinition(
        name,
        name,
        Fields.IGNORE_CASE.getDefaultValue(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty());
  }

  /** Decodes an AnalyzerDefinition from the supplied BsonDocument. */
  public static OverriddenBaseAnalyzerDefinition fromBson(BsonDocument document)
      throws BsonParseException {
    try (var parser = BsonDocumentParser.fromRoot(document).build()) {
      return fromBson(parser);
    }
  }

  /** Parse a AnalyzerDefinition from a DocumentParser. */
  public static OverriddenBaseAnalyzerDefinition fromBson(DocumentParser parser)
      throws BsonParseException {
    Optional<Set<String>> stopwords = parser.getField(Fields.STOPWORDS).unwrap().map(HashSet::new);
    Optional<Set<String>> stemExclusionSet =
        parser.getField(Fields.STEM_EXCLUSION_SET).unwrap().map(HashSet::new);

    var stopwordsPresent = stopwords.isPresent() && !stopwords.get().isEmpty();
    if (stemExclusionSet.isPresent() && !stopwordsPresent) {
      parser
          .getContext()
          .handleSemanticError("must have stopwords in order to support stem exclusion set");
    }

    return new OverriddenBaseAnalyzerDefinition(
        parser.getField(Fields.NAME).unwrap(),
        parser.getField(Fields.BASE_ANALYZER).unwrap(),
        parser.getField(Fields.IGNORE_CASE).unwrap(),
        parser.getField(Fields.MAX_TOKEN_LENGTH).unwrap(),
        stopwords,
        stemExclusionSet);
  }

  @Override
  public BsonDocument toBson() {
    Optional<List<String>> stopwords = this.stopwords.map(this::collectCharArraySetElements);
    Optional<List<String>> stemExclusionSet =
        this.stemExclusionSet.map(this::collectCharArraySetElements);

    return BsonDocumentBuilder.builder()
        .field(Fields.NAME, this.name)
        .field(Fields.BASE_ANALYZER, this.baseAnalyzerName)
        .field(Fields.IGNORE_CASE, this.ignoreCase)
        .field(Fields.MAX_TOKEN_LENGTH, this.maxTokenLength)
        .field(Fields.STOPWORDS, stopwords)
        .field(Fields.STEM_EXCLUSION_SET, stemExclusionSet)
        .build();
  }

  private List<String> collectCharArraySetElements(CharArraySet charArraySet) {
    return charArraySet.stream()
        .map(
            object -> {
              // First convert all of the members to char[], as that's what CharArraySet iterator()
              // returns elements of.
              if (!(object instanceof char[])) {
                throw new AssertionError("element in CharArraySet was not a char[]");
              }
              return (char[]) object;
            })
        .map(String::new)
        .collect(Collectors.toList());
  }

  @Override
  public String name() {
    return this.name;
  }

  public String getBaseAnalyzerName() {
    return this.baseAnalyzerName;
  }

  public boolean getIgnoreCase() {
    return this.ignoreCase;
  }

  /** Get max token length. */
  public Optional<Integer> getMaxTokenLength() {
    return this.maxTokenLength;
  }

  /** Get stopwords. */
  public Optional<CharArraySet> getStopwords() {
    return this.stopwords;
  }

  /** Get Stem Exclusion Set. */
  public Optional<CharArraySet> getStemExclusionSet() {
    return this.stemExclusionSet;
  }

  /** Test equality of two AnalyzerDefinitions. */
  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }

    if (!(other instanceof OverriddenBaseAnalyzerDefinition otherDefinition)) {
      return false;
    }

    return Objects.equals(this.name, otherDefinition.name)
        && Objects.equals(this.baseAnalyzerName, otherDefinition.baseAnalyzerName)
        && this.ignoreCase == otherDefinition.ignoreCase
        && Objects.equals(this.maxTokenLength, otherDefinition.maxTokenLength)
        && Objects.equals(this.stopwords, otherDefinition.stopwords)
        && Objects.equals(this.stemExclusionSet, otherDefinition.stemExclusionSet);
  }

  @Override
  public int hashCode() {
    // Note that stopwords and stemExclusionSet are not included in the hashCode calculation.
    // This is because their hashCode implementations end up delegating to
    // AbstractSet<char[]>.hashCode(), which in turn calls char[]::hashCode, which does not return a
    // consistent hashCode. As a result, including them results in inconsistent hashCodes for
    // AnalyzerDefinition.
    // Not including them still upholds the contract between equals() and hashCode(), it just makes
    // the likelihood of collisions more likely.
    return Objects.hash(this.name, this.baseAnalyzerName, this.ignoreCase, this.maxTokenLength);
  }
}
