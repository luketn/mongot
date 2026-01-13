package com.xgen.testing.mongot.integration.index.serialization;

import static com.xgen.mongot.index.definition.IndexDefinition.Fields.NUM_PARTITIONS;

import com.xgen.mongot.index.analyzer.definition.CustomAnalyzerDefinition;
import com.xgen.mongot.index.analyzer.definition.OverriddenBaseAnalyzerDefinition;
import com.xgen.mongot.index.definition.DocumentFieldDefinition;
import com.xgen.mongot.index.definition.StoredSourceDefinition;
import com.xgen.mongot.index.definition.SynonymMappingDefinition;
import com.xgen.mongot.index.definition.SynonymSourceDefinition;
import com.xgen.mongot.index.definition.TypeSetDefinition;
import com.xgen.mongot.index.query.sort.Sort;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.Value;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;

public class SearchIndexSpec extends IndexSpec {

  public static final BsonDocument DEFAULT_MAPPINGS =
      new BsonDocument("dynamic", new BsonBoolean(true));

  static class Fields {
    static final Field.Optional<String> ANALYZER =
        Field.builder("analyzer").stringField().mustNotBeEmpty().optional().noDefault();

    static final Field.Optional<String> SEARCH_ANALYZER =
        Field.builder("searchAnalyzer").stringField().mustNotBeEmpty().optional().noDefault();

    static final Field.Optional<BsonDocument> MAPPINGS =
        Field.builder("mappings").documentField().optional().noDefault();

    static final Field.WithDefault<List<OverriddenBaseAnalyzerDefinition>> OVERRIDDEN_ANALYZERS =
        Field.builder("overriddenAnalyzers")
            .classField(OverriddenBaseAnalyzerDefinition::fromBson)
            .disallowUnknownFields()
            .asList()
            .optional()
            .withDefault(Collections.emptyList());

    static final Field.WithDefault<List<CustomAnalyzerDefinition>> ANALYZERS =
        Field.builder("analyzers")
            .classField(CustomAnalyzerDefinition::fromBson)
            .disallowUnknownFields()
            .asList()
            .optional()
            .withDefault(Collections.emptyList());

    static final Field.Optional<List<SynonymMappingDefinition>> SYNONYMS =
        Field.builder("synonyms")
            .classField(SynonymMappingDefinition::fromBson)
            .disallowUnknownFields()
            .asList()
            .mustHaveUniqueAttribute("name", definition -> definition.name())
            .optional()
            .noDefault();

    static final Field.Optional<StoredSourceDefinition> STORED_SOURCE =
        Field.builder("storedSource")
            .classField(StoredSourceDefinition::fromBson)
            .optional()
            .noDefault();

    static final Field.Optional<List<TypeSetDefinition>> TYPE_SETS =
        Field.builder("typeSets")
            .listOf(
                Value.builder()
                    .classValue(TypeSetDefinition::fromBson)
                    .disallowUnknownFields()
                    .required())
            .mustHaveUniqueAttribute("name", TypeSetDefinition::name)
            .mustNotBeEmpty()
            .optional()
            .noDefault();

    static final Field.WithDefault<SearchIndexRunForSpec> RUN_FOR =
        Field.builder("runFor")
            .classField(SearchIndexRunForSpec::fromBson)
            .disallowUnknownFields()
            .optional()
            .withDefault(SearchIndexRunForSpec.DEFAULT);

    static final Field.Optional<Sort> SORT =
        Field.builder("sort").classField(Sort::fromBsonAsSort).optional().noDefault();
  }

  public static final SearchIndexSpec EMPTY =
      new SearchIndexSpec(
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          Collections.emptyList(),
          Collections.emptyList(),
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          Fields.RUN_FOR.getDefaultValue(),
          NUM_PARTITIONS.getDefaultValue(),
          Optional.empty());
  private final Optional<String> analyzer;
  private final Optional<String> searchAnalyzer;
  private final Optional<BsonDocument> mappings;
  private final List<OverriddenBaseAnalyzerDefinition> overriddenAnalyzers;
  private final List<CustomAnalyzerDefinition> customAnalyzers;
  private final Optional<List<SynonymMappingDefinition>> synonyms;
  private final Optional<StoredSourceDefinition> storedSource;
  private final Optional<List<TypeSetDefinition>> typeSets;
  private final SearchIndexRunForSpec runFor;
  private final int numPartitions;
  private final Optional<Sort> sort;

  private SearchIndexSpec(
      Optional<String> analyzer,
      Optional<String> searchAnalyzer,
      Optional<BsonDocument> mappings,
      List<OverriddenBaseAnalyzerDefinition> overriddenAnalyzers,
      List<CustomAnalyzerDefinition> customAnalyzers,
      Optional<List<SynonymMappingDefinition>> synonyms,
      Optional<StoredSourceDefinition> storedSource,
      Optional<List<TypeSetDefinition>> typeSets,
      SearchIndexRunForSpec runFor,
      int numPartitions,
      Optional<Sort> sort) {
    this.analyzer = analyzer;
    this.searchAnalyzer = searchAnalyzer;
    this.mappings = mappings;
    this.overriddenAnalyzers = overriddenAnalyzers;
    this.customAnalyzers = customAnalyzers;
    this.synonyms = synonyms;
    this.storedSource = storedSource;
    this.typeSets = typeSets;
    this.runFor = runFor;
    this.numPartitions = numPartitions;
    this.sort = sort;
  }

  @Override
  public Type getType() {
    return Type.SEARCH;
  }

  /** Builds IndexSpec from Bson. */
  public static SearchIndexSpec fromBson(DocumentParser parser) throws BsonParseException {
    return new SearchIndexSpec(
        parser.getField(Fields.ANALYZER).unwrap(),
        parser.getField(Fields.SEARCH_ANALYZER).unwrap(),
        parser.getField(Fields.MAPPINGS).unwrap(),
        parser.getField(Fields.OVERRIDDEN_ANALYZERS).unwrap(),
        parser.getField(Fields.ANALYZERS).unwrap(),
        parser.getField(Fields.SYNONYMS).unwrap(),
        parser.getField(Fields.STORED_SOURCE).unwrap(),
        parser.getField(Fields.TYPE_SETS).unwrap(),
        parser.getField(Fields.RUN_FOR).unwrap(),
        parser.getField(NUM_PARTITIONS).unwrap(),
        parser.getField(Fields.SORT).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    var builder = BsonDocumentBuilder.builder();

    builder.field(Fields.RUN_FOR, this.runFor);

    this.analyzer.ifPresent(analyzer -> builder.field(Fields.ANALYZER, Optional.of(analyzer)));
    this.mappings.ifPresent(mappings -> builder.field(Fields.MAPPINGS, Optional.of(mappings)));

    if (!this.overriddenAnalyzers.isEmpty()) {
      builder.field(Fields.OVERRIDDEN_ANALYZERS, this.overriddenAnalyzers);
    }

    if (!this.customAnalyzers.isEmpty()) {
      builder.field(Fields.ANALYZERS, this.customAnalyzers);
    }

    this.synonyms.ifPresent(synonyms -> builder.field(Fields.SYNONYMS, Optional.of(synonyms)));
    this.storedSource.ifPresent(
        storedSource -> builder.field(Fields.STORED_SOURCE, Optional.of(storedSource)));

    builder.field(NUM_PARTITIONS, this.numPartitions);

    if (this.sort.isPresent()) {
      builder.field(Fields.SORT, this.sort);
    }

    return builder.build();
  }

  SearchIndexSpec withPrefixedSynonymMappingCollections(String testName) {
    if (this.synonyms.isEmpty()) {
      return this;
    }
    // rewrite synonyms collection names to be prefixed with testName
    var synonymMappingsWithTestNamePrefixedCollections =
        this.synonyms.get().stream()
            .map(
                mapping ->
                    new SynonymMappingDefinition(
                        mapping.name(),
                        new SynonymSourceDefinition(
                            synonymCollectionName(testName, mapping.source().collection())),
                        mapping.analyzer()))
            .collect(Collectors.toList());

    return new SearchIndexSpec(
        this.analyzer,
        this.searchAnalyzer,
        this.mappings,
        this.overriddenAnalyzers,
        this.customAnalyzers,
        Optional.of(synonymMappingsWithTestNamePrefixedCollections),
        this.storedSource,
        this.typeSets,
        this.runFor,
        this.numPartitions,
        this.sort);
  }

  static String synonymCollectionName(String testName, String synonymCollectionName) {
    return String.format("%s-syns-%s", testName, synonymCollectionName);
  }

  @Override
  public List<IndexFormatVersion> getIndexFormatVersions() {
    int from = this.runFor.getIndexFormatVersion().getFrom();
    int to = this.runFor.getIndexFormatVersion().getTo();
    return IntStream.rangeClosed(from, to).mapToObj(IndexFormatVersion::create).toList();
  }

  public Optional<String> getAnalyzer() {
    return this.analyzer;
  }

  public Optional<String> getSearchAnalyzer() {
    return this.searchAnalyzer;
  }

  public BsonDocument getMappings() {
    return this.mappings.orElse(DEFAULT_MAPPINGS);
  }

  public DocumentFieldDefinition parseMappings() {
    try (var parser = BsonDocumentParser.fromRoot(getMappings()).build()) {
      return DocumentFieldDefinition.fromBson(parser);
    } catch (BsonParseException e) {
      throw new RuntimeException(e);
    }
  }

  public List<OverriddenBaseAnalyzerDefinition> getOverriddenAnalyzers() {
    return this.overriddenAnalyzers;
  }

  public List<CustomAnalyzerDefinition> getCustomAnalyzers() {
    return this.customAnalyzers;
  }

  public Optional<List<SynonymMappingDefinition>> getSynonyms() {
    return this.synonyms;
  }

  public Optional<StoredSourceDefinition> getStoredSource() {
    return this.storedSource;
  }

  public Optional<List<TypeSetDefinition>> getTypeSets() {
    return this.typeSets;
  }

  public Optional<Sort> getSort() {
    return this.sort;
  }

  @Override
  public List<Integer> getIndexFeatureVersions() {
    int from = this.runFor.getIndexFeatureVersion().getFrom();
    int to = this.runFor.getIndexFeatureVersion().getTo();
    return IntStream.rangeClosed(from, to).boxed().toList();
  }

  public boolean isDynamic() {
    return this.getMappings().getBoolean("dynamic", new BsonBoolean(false)).getValue();
  }

  @Override
  public int getNumPartitions() {
    return this.numPartitions;
  }
}
