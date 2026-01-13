package com.xgen.mongot.index.definition;

import com.google.common.collect.ImmutableList;
import com.xgen.mongot.index.analyzer.definition.AnalyzerDefinition;
import com.xgen.mongot.index.analyzer.definition.OverriddenBaseAnalyzerDefinition;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.bson.BsonDocument;

/**
 * Couples an IndexDefinition with the overridden analyzer definitions it references. This object
 * ensures that all the references are satisfied.
 */
public record AnalyzerBoundSearchIndexDefinition(
    SearchIndexDefinition indexDefinition,
    ImmutableList<OverriddenBaseAnalyzerDefinition> analyzerDefinitions)
    implements DocumentEncodable {
  static class Fields {

    static final Field.Required<SearchIndexDefinition> INDEX =
        Field.builder("index")
            .classField(SearchIndexDefinition::fromBson)
            .disallowUnknownFields()
            .required();

    static final Field.Required<List<OverriddenBaseAnalyzerDefinition>> ANALYZERS =
        Field.builder("analyzers")
            .classField(OverriddenBaseAnalyzerDefinition::fromBson)
            .disallowUnknownFields()
            .asList()
            .required();
  }

  private AnalyzerBoundSearchIndexDefinition(
      SearchIndexDefinition index, List<OverriddenBaseAnalyzerDefinition> analyzers) {
    this(index, ImmutableList.copyOf(analyzers));
  }

  /**
   * Deserialize. Throws BsonParseException if the analyzers do not satisfy the overridden analyzers
   * required by the index.
   */
  public static AnalyzerBoundSearchIndexDefinition fromBson(DocumentParser parser)
      throws BsonParseException {
    var index = parser.getField(Fields.INDEX).unwrap();
    var analyzers = parser.getField(Fields.ANALYZERS).unwrap();

    Optional<String> referenceMismatch = analyzerReferenceMismatch(index, analyzers);
    if (referenceMismatch.isPresent()) {
      parser.getContext().handleSemanticError(referenceMismatch.get());
    }

    return new AnalyzerBoundSearchIndexDefinition(index, analyzers);
  }

  /**
   * Only take the referenced overridden analyzers (along with custom analyzers defined in the
   * IndexDefinition) to construct a AnalyzerBoundIndexDefinition. Throws IllegalArgumentException
   * if referenced analyzers are missing.
   */
  public static AnalyzerBoundSearchIndexDefinition withRelevantOverriddenAnalyzers(
      SearchIndexDefinition definition,
      List<OverriddenBaseAnalyzerDefinition> overriddenAnalyzers) {
    Set<String> nonStockAnalyzerNames = definition.getNonStockAnalyzerNames();
    ImmutableList<OverriddenBaseAnalyzerDefinition> relevantOverriddenAnalyzers =
        overriddenAnalyzers.stream()
            .filter(analyzer -> nonStockAnalyzerNames.contains(analyzer.name()))
            .collect(ImmutableList.toImmutableList());
    return create(definition, relevantOverriddenAnalyzers);
  }

  /** Throws if analyzers do not satisfy the invariants. */
  public static AnalyzerBoundSearchIndexDefinition create(
      SearchIndexDefinition index, List<OverriddenBaseAnalyzerDefinition> analyzers) {
    Optional<String> referenceMismatch = analyzerReferenceMismatch(index, analyzers);
    if (referenceMismatch.isPresent()) {
      throw new IllegalArgumentException(referenceMismatch.get());
    }

    return new AnalyzerBoundSearchIndexDefinition(index, analyzers);
  }

  private static Optional<String> analyzerReferenceMismatch(
      SearchIndexDefinition index, List<OverriddenBaseAnalyzerDefinition> analyzers) {
    Set<String> referenced = index.getNonStockAnalyzerNames();
    Set<String> overridden =
        analyzers.stream().map(OverriddenBaseAnalyzerDefinition::name).collect(Collectors.toSet());
    Set<String> supplied = analyzerNamesDefinedIn(index, analyzers);

    // All overridden analyzers should be referenced in an AnalyzerBoundIndexDefinition - otherwise
    // they should not be "bound" to this AnalyzerBoundIndexDefinition.
    if (supplied.containsAll(referenced) && referenced.containsAll(overridden)) {
      return Optional.empty();
    } else {
      return Optional.of(
          String.format("missing or unneeded analyzers %s =! %s", referenced, supplied));
    }
  }

  private static Set<String> analyzerNamesDefinedIn(
      SearchIndexDefinition indexDefinition,
      List<OverriddenBaseAnalyzerDefinition> overriddenAnalyzerDefinitions) {

    return Stream.concat(
            overriddenAnalyzerDefinitions.stream(), indexDefinition.getAnalyzers().stream())
        .map(AnalyzerDefinition::name)
        .collect(Collectors.toSet());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.INDEX, this.indexDefinition)
        .field(Fields.ANALYZERS, this.analyzerDefinitions)
        .build();
  }
}
