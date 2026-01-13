package com.xgen.mongot.index.analyzer.wrapper;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.mongot.index.definition.SearchIndexDefinition.DEFAULT_FALLBACK_ANALYZER;
import static com.xgen.mongot.index.definition.SearchIndexDefinition.DEFAULT_FALLBACK_NORMALIZER;

import com.xgen.mongot.index.analyzer.AnalyzerRegistry;
import com.xgen.mongot.index.analyzer.InvalidAnalyzerDefinitionException;
import com.xgen.mongot.index.analyzer.definition.AnalyzerDefinition;
import com.xgen.mongot.index.analyzer.definition.NormalizerDefinition;
import com.xgen.mongot.index.analyzer.definition.StockAnalyzerNames;
import com.xgen.mongot.index.analyzer.definition.StockNormalizerName;
import com.xgen.mongot.index.definition.AutocompleteFieldDefinition;
import com.xgen.mongot.index.definition.DocumentFieldDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.StringFieldDefinition;
import com.xgen.mongot.index.definition.TokenFieldDefinition;
import com.xgen.mongot.index.definition.TypeSetDefinition;
import com.xgen.testing.mongot.index.analyzer.definition.OverriddenBaseAnalyzerDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.AutocompleteFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.EmbeddedDocumentsFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.StringFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.TokenFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.TypeSetDefinitionBuilder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.lucene.analysis.Analyzer;
import org.junit.Test;

public class DynamicMappingsTest {

  private static AnalyzerRegistry getAnalyzerRegistry() throws InvalidAnalyzerDefinitionException {
    List<AnalyzerDefinition> analyzers = new ArrayList<>();
    for (String name :
        Arrays.asList(
            "search",
            "index",
            "fieldAnalyzer0",
            "fieldAnalyzer1",
            "fieldSearchAnalyzer",
            "multiAnalyzer0",
            "multiAnalyzer1",
            "autocomplete0",
            "autocomplete1")) {
      analyzers.add(
          OverriddenBaseAnalyzerDefinitionBuilder.builder()
              .baseAnalyzerName(StockAnalyzerNames.LUCENE_STANDARD.getName())
              .name(name)
              .build());
    }

    analyzers.add(NormalizerDefinition.stockNormalizer(StockNormalizerName.LOWERCASE));
    return AnalyzerRegistry.factory().create(analyzers, true);
  }

  static final StringFieldDefinition STRING_FIELD_WITH_ANALYZER0_AND_MULTI =
      StringFieldDefinitionBuilder.builder()
          .analyzerName("fieldAnalyzer0")
          .multi(
              "multi0",
              StringFieldDefinitionBuilder.builder().analyzerName("multiAnalyzer0").build())
          .multi(
              "multi1",
              StringFieldDefinitionBuilder.builder().analyzerName("multiAnalyzer1").build())
          .build();
  static final StringFieldDefinition STRING_FIELD_WITH_ANALYZER1 =
      StringFieldDefinitionBuilder.builder().analyzerName("fieldAnalyzer1").build();

  static final TokenFieldDefinition TOKEN_FIELD_DEF_WITH_NONE_NORMALIZER =
      TokenFieldDefinitionBuilder.builder().normalizerName(StockNormalizerName.NONE).build();
  static final TokenFieldDefinition TOKEN_FIELD_DEF_WITH_LOWERCASE_NORMALIZER =
      TokenFieldDefinitionBuilder.builder().normalizerName(StockNormalizerName.LOWERCASE).build();

  static final AutocompleteFieldDefinition AUTOCOMPLETE_FIELD_WITH_ANALYZER0 =
      AutocompleteFieldDefinitionBuilder.builder().analyzer("autocomplete0").build();
  static final AutocompleteFieldDefinition AUTOCOMPLETE_FIELD_WITH_ANALYZER1 =
      AutocompleteFieldDefinitionBuilder.builder().analyzer("autocomplete1").build();

  static final TypeSetDefinition TYPESET_WITH_ALL_FIELD_TYPES0 =
      TypeSetDefinitionBuilder.builder()
          .name("TYPESET_WITH_ALL_FIELD_TYPES0")
          .addType(STRING_FIELD_WITH_ANALYZER0_AND_MULTI)
          .addType(TOKEN_FIELD_DEF_WITH_NONE_NORMALIZER)
          .addType(AUTOCOMPLETE_FIELD_WITH_ANALYZER0)
          .build();

  static final TypeSetDefinition TYPESET_WITH_ALL_FIELD_TYPES1 =
      TypeSetDefinitionBuilder.builder()
          .name("TYPESET_WITH_ALL_FIELD_TYPES1")
          .addType(STRING_FIELD_WITH_ANALYZER1)
          .addType(TOKEN_FIELD_DEF_WITH_LOWERCASE_NORMALIZER)
          .addType(AUTOCOMPLETE_FIELD_WITH_ANALYZER1)
          .build();

  static final TypeSetDefinition TYPESET_WITH_ONLY_STRING_MULTI =
      TypeSetDefinitionBuilder.builder()
          .name("TYPESET_WITH_ONLY_STRING_MULTI")
          .addType(STRING_FIELD_WITH_ANALYZER0_AND_MULTI)
          .build();

  static final TypeSetDefinition TYPESET_WITH_ONLY_STRING =
      TypeSetDefinitionBuilder.builder()
          .name("TYPESET_WITH_ONLY_STRING")
          .addType(STRING_FIELD_WITH_ANALYZER1)
          .build();

  static final TypeSetDefinition TYPESET_WITH_ONLY_TOKEN =
      TypeSetDefinitionBuilder.builder()
          .name("TYPESET_WITH_ONLY_TOKEN")
          .addType(TOKEN_FIELD_DEF_WITH_LOWERCASE_NORMALIZER)
          .build();

  static final TypeSetDefinition TYPESET_WITH_ONLY_AUTOCOMPLETE =
      TypeSetDefinitionBuilder.builder()
          .name("TYPESET_WITH_ONLY_AUTOCOMPLETE")
          .addType(AUTOCOMPLETE_FIELD_WITH_ANALYZER0)
          .build();

  @Test
  public void rootLevelDynamicSetToFalse() throws Exception {
    DocumentFieldDefinition mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "stringDocument",
                FieldDefinitionBuilder.builder()
                    .document(
                        DocumentFieldDefinitionBuilder.builder()
                            .dynamic("TYPESET_WITH_ONLY_STRING")
                            .build())
                    .build())
            .field(
                "tokenDocument",
                FieldDefinitionBuilder.builder()
                    .document(
                        DocumentFieldDefinitionBuilder.builder()
                            .dynamic("TYPESET_WITH_ONLY_TOKEN")
                            .build())
                    .build())
            .field(
                "autocompleteDocument",
                FieldDefinitionBuilder.builder()
                    .document(
                        DocumentFieldDefinitionBuilder.builder()
                            .dynamic("TYPESET_WITH_ONLY_AUTOCOMPLETE")
                            .build())
                    .build())
            .build();

    SearchIndexDefinition index =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .mappings(mappings)
            .typeSets(
                TYPESET_WITH_ONLY_STRING, TYPESET_WITH_ONLY_TOKEN, TYPESET_WITH_ONLY_AUTOCOMPLETE)
            .analyzerName("index")
            .searchAnalyzerName("search")
            .build();

    AnalyzerRegistry registry = getAnalyzerRegistry();
    Analyzer fallback = registry.getAnalyzer(DEFAULT_FALLBACK_ANALYZER);
    Analyzer fallbackNormalizer = registry.getNormalizer(DEFAULT_FALLBACK_NORMALIZER);

    DynamicTypeSetPrefixMap<Analyzer> dynamicTypeSetPrefixMap =
        new IndexAnalyzerBuilder()
            .buildDynamicMappings(index, registry, fallback, fallbackNormalizer)
            .get();

    assertThat(dynamicTypeSetPrefixMap.getNearestRoot("$type:token/another.field")).isEmpty();

    assertThat(dynamicTypeSetPrefixMap.getNearestRoot("$type:string/stringDocument.a"))
        .hasValue(registry.getAnalyzer("fieldAnalyzer1"));
    assertThat(dynamicTypeSetPrefixMap.getNearestRoot("$type:token/stringDocument.a")).isEmpty();
    assertThat(dynamicTypeSetPrefixMap.getNearestRoot("$type:autocomplete/stringDocument.a"))
        .isEmpty();

    assertThat(dynamicTypeSetPrefixMap.getNearestRoot("$type:token/tokenDocument.a"))
        .hasValue(registry.getNormalizer(StockNormalizerName.LOWERCASE));
    assertThat(dynamicTypeSetPrefixMap.getNearestRoot("$type:string/tokenDocument.a")).isEmpty();
    assertThat(dynamicTypeSetPrefixMap.getNearestRoot("$type:autocomplete/tokenDocument.a"))
        .isEmpty();

    assertThat(dynamicTypeSetPrefixMap.getNearestRoot("$type:autocomplete/autocompleteDocument.a"))
        .hasValue(registry.getAutocompleteAnalyzer(AUTOCOMPLETE_FIELD_WITH_ANALYZER0));
    assertThat(dynamicTypeSetPrefixMap.getNearestRoot("$type:string/autocompleteDocument.a"))
        .isEmpty();
    assertThat(dynamicTypeSetPrefixMap.getNearestRoot("$type:token/autocompleteDocument.a"))
        .isEmpty();

    assertThat(dynamicTypeSetPrefixMap.getNearestRoot("$type:autocomplete/autocompleteDocument"))
        .isEmpty();
    assertThat(dynamicTypeSetPrefixMap.getNearestRoot("$type:string/stringDocument")).isEmpty();
    assertThat(dynamicTypeSetPrefixMap.getNearestRoot("$type:token/tokenDocument")).isEmpty();
  }

  @Test
  public void documentsWithOnlyMappings() throws Exception {
    DocumentFieldDefinition mappings =
        DocumentFieldDefinitionBuilder.builder().dynamic("TYPESET_WITH_ALL_FIELD_TYPES0").build();

    SearchIndexDefinition index =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .mappings(mappings)
            .typeSets(TYPESET_WITH_ALL_FIELD_TYPES0)
            .analyzerName("index")
            .searchAnalyzerName("search")
            .build();

    AnalyzerRegistry registry = getAnalyzerRegistry();
    Analyzer fallback = registry.getAnalyzer(DEFAULT_FALLBACK_ANALYZER);
    Analyzer fallbackNormalizer = registry.getNormalizer(DEFAULT_FALLBACK_NORMALIZER);

    DynamicTypeSetPrefixMap<Analyzer> dynamicTypeSetPrefixMap =
        new IndexAnalyzerBuilder()
            .buildDynamicMappings(index, registry, fallback, fallbackNormalizer)
            .get();

    assertThat(dynamicTypeSetPrefixMap.getNearestRoot("$type:string/field0"))
        .hasValue(registry.getAnalyzer("fieldAnalyzer0"));
    assertThat(dynamicTypeSetPrefixMap.getNearestRoot("$multi/field0.field1.multi0"))
        .hasValue(registry.getAnalyzer("multiAnalyzer0"));
    assertThat(dynamicTypeSetPrefixMap.getNearestRoot("$type:token/another.field"))
        .hasValue(registry.getNormalizer(StockNormalizerName.NONE));
    assertThat(dynamicTypeSetPrefixMap.getNearestRoot("$type:autocomplete/a"))
        .hasValue(registry.getAutocompleteAnalyzer(AUTOCOMPLETE_FIELD_WITH_ANALYZER0));
  }

  @Test
  public void documentsWithNestedFields() throws Exception {
    DocumentFieldDefinition mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic("TYPESET_WITH_ALL_FIELD_TYPES0")
            .field(
                "field0",
                FieldDefinitionBuilder.builder()
                    .document(
                        DocumentFieldDefinitionBuilder.builder()
                            .dynamic("TYPESET_WITH_ALL_FIELD_TYPES1")
                            .field(
                                "field1",
                                FieldDefinitionBuilder.builder()
                                    .document(
                                        DocumentFieldDefinitionBuilder.builder()
                                            .dynamic("TYPESET_WITH_ONLY_AUTOCOMPLETE")
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();

    SearchIndexDefinition index =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .mappings(mappings)
            .typeSets(
                TYPESET_WITH_ALL_FIELD_TYPES0,
                TYPESET_WITH_ALL_FIELD_TYPES1,
                TYPESET_WITH_ONLY_AUTOCOMPLETE)
            .analyzerName("index")
            .searchAnalyzerName("search")
            .build();

    AnalyzerRegistry registry = getAnalyzerRegistry();
    Analyzer fallback = registry.getAnalyzer(DEFAULT_FALLBACK_ANALYZER);
    Analyzer fallbackNormalizer = registry.getNormalizer(DEFAULT_FALLBACK_NORMALIZER);

    DynamicTypeSetPrefixMap<Analyzer> dynamicTypeSetPrefixMap =
        new IndexAnalyzerBuilder()
            .buildDynamicMappings(index, registry, fallback, fallbackNormalizer)
            .get();

    assertThat(dynamicTypeSetPrefixMap.getNearestRoot("$type:string/a"))
        .hasValue(registry.getAnalyzer("fieldAnalyzer0"));
    assertThat(dynamicTypeSetPrefixMap.getNearestRoot("$multi/a.multi0"))
        .hasValue(registry.getAnalyzer("multiAnalyzer0"));
    assertThat(dynamicTypeSetPrefixMap.getNearestRoot("$type:token/a"))
        .hasValue(registry.getNormalizer(StockNormalizerName.NONE));
    assertThat(dynamicTypeSetPrefixMap.getNearestRoot("$type:autocomplete/a"))
        .hasValue(registry.getAutocompleteAnalyzer(AUTOCOMPLETE_FIELD_WITH_ANALYZER0));

    assertThat(dynamicTypeSetPrefixMap.getNearestRoot("$type:string/field0.a"))
        .hasValue(registry.getAnalyzer("fieldAnalyzer1"));
    assertThat(dynamicTypeSetPrefixMap.getNearestRoot("$type:token/field0.a"))
        .hasValue(registry.getNormalizer(StockNormalizerName.LOWERCASE));
    assertThat(dynamicTypeSetPrefixMap.getNearestRoot("$type:autocomplete/field0.a"))
        .hasValue(registry.getAutocompleteAnalyzer(AUTOCOMPLETE_FIELD_WITH_ANALYZER1));

    assertThat(dynamicTypeSetPrefixMap.getNearestRoot("$type:string/field0.anotherField.a"))
        .hasValue(registry.getAnalyzer("fieldAnalyzer1"));
    assertThat(dynamicTypeSetPrefixMap.getNearestRoot("$type:token/field0.another.another.field.a"))
        .hasValue(registry.getNormalizer(StockNormalizerName.LOWERCASE));
    assertThat(dynamicTypeSetPrefixMap.getNearestRoot("$type:autocomplete/field0.field1.a"))
        .hasValue(registry.getAutocompleteAnalyzer(AUTOCOMPLETE_FIELD_WITH_ANALYZER0));
  }

  @Test
  public void embeddedDocumentsWithNestedFields() throws Exception {
    DocumentFieldDefinition mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(true)
            .field(
                "field0",
                FieldDefinitionBuilder.builder()
                    .embeddedDocuments(
                        EmbeddedDocumentsFieldDefinitionBuilder.builder()
                            .dynamic("TYPESET_WITH_ALL_FIELD_TYPES0")
                            .field(
                                "field1",
                                FieldDefinitionBuilder.builder()
                                    .embeddedDocuments(
                                        EmbeddedDocumentsFieldDefinitionBuilder.builder()
                                            .dynamic("TYPESET_WITH_ALL_FIELD_TYPES1")
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();

    SearchIndexDefinition index =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .mappings(mappings)
            .typeSets(TYPESET_WITH_ALL_FIELD_TYPES0, TYPESET_WITH_ALL_FIELD_TYPES1)
            .analyzerName("index")
            .searchAnalyzerName("search")
            .build();

    AnalyzerRegistry registry = getAnalyzerRegistry();
    Analyzer fallback = registry.getAnalyzer(DEFAULT_FALLBACK_ANALYZER);
    Analyzer fallbackNormalizer = registry.getNormalizer(DEFAULT_FALLBACK_NORMALIZER);

    DynamicTypeSetPrefixMap<Analyzer> dynamicTypeSetPrefixMap =
        new IndexAnalyzerBuilder()
            .buildDynamicMappings(index, registry, fallback, fallbackNormalizer)
            .get();

    assertThat(dynamicTypeSetPrefixMap.getNearestRoot("$embedded:6/field0/$type:string/field0.a"))
        .hasValue(registry.getAnalyzer("fieldAnalyzer0"));
    assertThat(dynamicTypeSetPrefixMap.getNearestRoot("$embedded:6/field0/$multi/field0.a.multi0"))
        .hasValue(registry.getAnalyzer("multiAnalyzer0"));
    assertThat(dynamicTypeSetPrefixMap.getNearestRoot("$embedded:6/field0/$type:token/field0.a"))
        .hasValue(registry.getNormalizer(StockNormalizerName.NONE));
    assertThat(
            dynamicTypeSetPrefixMap.getNearestRoot(
                "$embedded:6/field0/$type:autocomplete/field0.a"))
        .hasValue(registry.getAutocompleteAnalyzer(AUTOCOMPLETE_FIELD_WITH_ANALYZER0));

    assertThat(
            dynamicTypeSetPrefixMap.getNearestRoot(
                "$embedded:6/field0/$type:string/field0.anotherField.a"))
        .hasValue(registry.getAnalyzer("fieldAnalyzer0"));
    assertThat(
            dynamicTypeSetPrefixMap.getNearestRoot(
                "$embedded:6/field0/$multi/field0.another.field.multi0"))
        .hasValue(registry.getAnalyzer("multiAnalyzer0"));
    assertThat(
            dynamicTypeSetPrefixMap.getNearestRoot(
                "$embedded:6/field0/$type:token/field0.another.another.field.a"))
        .hasValue(registry.getNormalizer(StockNormalizerName.NONE));
    assertThat(
            dynamicTypeSetPrefixMap.getNearestRoot(
                "$embedded:6/field0/$type:autocomplete/field0.another.another.field.a"))
        .hasValue(registry.getAutocompleteAnalyzer(AUTOCOMPLETE_FIELD_WITH_ANALYZER0));

    assertThat(
            dynamicTypeSetPrefixMap.getNearestRoot(
                "$embedded:13/field0.field1/$type:string/field0.field1.anotherField.a"))
        .hasValue(registry.getAnalyzer("fieldAnalyzer1"));
    assertThat(
            dynamicTypeSetPrefixMap.getNearestRoot(
                "$embedded:13/field0.field1/$type:token/field0.field1.another.another.field.a"))
        .hasValue(registry.getNormalizer(StockNormalizerName.LOWERCASE));
    assertThat(
            dynamicTypeSetPrefixMap.getNearestRoot(
                "$embedded:13/field0.field1/$type:autocomplete/field0.field1.a.b.c"))
        .hasValue(registry.getAutocompleteAnalyzer(AUTOCOMPLETE_FIELD_WITH_ANALYZER1));
  }

  @Test
  public void documentsAndEmbeddedDocuments() throws Exception {
    DocumentFieldDefinition mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(true)
            .field(
                "field0",
                FieldDefinitionBuilder.builder()
                    .embeddedDocuments(
                        EmbeddedDocumentsFieldDefinitionBuilder.builder()
                            .dynamic("TYPESET_WITH_ALL_FIELD_TYPES0")
                            .field(
                                "field1",
                                FieldDefinitionBuilder.builder()
                                    .embeddedDocuments(
                                        EmbeddedDocumentsFieldDefinitionBuilder.builder()
                                            .dynamic("TYPESET_WITH_ONLY_STRING_MULTI")
                                            .field(
                                                "field2",
                                                FieldDefinitionBuilder.builder()
                                                    .document(
                                                        DocumentFieldDefinitionBuilder.builder()
                                                            .dynamic(
                                                                "TYPESET_WITH_ALL_FIELD_TYPES1")
                                                            .build())
                                                    .build())
                                            .build())
                                    .document(
                                        DocumentFieldDefinitionBuilder.builder()
                                            .dynamic("TYPESET_WITH_ONLY_STRING")
                                            .field(
                                                "field2",
                                                FieldDefinitionBuilder.builder()
                                                    .embeddedDocuments(
                                                        EmbeddedDocumentsFieldDefinitionBuilder
                                                            .builder()
                                                            .dynamic("TYPESET_WITH_ONLY_TOKEN")
                                                            .build())
                                                    .document(
                                                        DocumentFieldDefinitionBuilder.builder()
                                                            .dynamic(
                                                                "TYPESET_WITH_ONLY_AUTOCOMPLETE")
                                                            .build())
                                                    .build())
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();

    SearchIndexDefinition index =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .mappings(mappings)
            .typeSets(
                TYPESET_WITH_ALL_FIELD_TYPES0,
                TYPESET_WITH_ALL_FIELD_TYPES1,
                TYPESET_WITH_ONLY_STRING_MULTI,
                TYPESET_WITH_ONLY_STRING,
                TYPESET_WITH_ONLY_TOKEN,
                TYPESET_WITH_ONLY_AUTOCOMPLETE)
            .analyzerName("index")
            .searchAnalyzerName("search")
            .build();

    AnalyzerRegistry registry = getAnalyzerRegistry();
    Analyzer fallback = registry.getAnalyzer(DEFAULT_FALLBACK_ANALYZER);
    Analyzer fallbackNormalizer = registry.getNormalizer(DEFAULT_FALLBACK_NORMALIZER);

    DynamicTypeSetPrefixMap<Analyzer> dynamicTypeSetPrefixMap =
        new IndexAnalyzerBuilder()
            .buildDynamicMappings(index, registry, fallback, fallbackNormalizer)
            .get();

    assertThat(dynamicTypeSetPrefixMap.getNearestRoot("$type:token/field0.a.b.c")).isEmpty();

    assertThat(
            dynamicTypeSetPrefixMap.getNearestRoot(
                "$embedded:6/field0/$type:token/field0.token.field"))
        .hasValue(registry.getNormalizer(StockNormalizerName.NONE));
    assertThat(
            dynamicTypeSetPrefixMap.getNearestRoot("$embedded:6/field0/$type:token/field0.a.b.c"))
        .hasValue(registry.getNormalizer(StockNormalizerName.NONE));
    assertThat(
            dynamicTypeSetPrefixMap.getNearestRoot(
                "$embedded:6/field0/$type:string/field0.string.field"))
        .hasValue(registry.getAnalyzer("fieldAnalyzer0"));
    assertThat(
            dynamicTypeSetPrefixMap.getNearestRoot(
                "$embedded:6/field0/$type:autocomplete/field0.a.b.c"))
        .hasValue(registry.getAutocompleteAnalyzer(AUTOCOMPLETE_FIELD_WITH_ANALYZER0));

    assertThat(
            dynamicTypeSetPrefixMap.getNearestRoot(
                "$embedded:6/field0/$type:string/field0.field1.string.field"))
        .hasValue(registry.getAnalyzer("fieldAnalyzer1"));

    assertThat(
            dynamicTypeSetPrefixMap.getNearestRoot(
                "$embedded:13/field0.field1/$type:string/field0.field1.string.field"))
        .hasValue(registry.getAnalyzer("fieldAnalyzer0"));
    assertThat(
            dynamicTypeSetPrefixMap.getNearestRoot(
                "$embedded:13/field0.field1/$multi/field0.field1.a.multi0"))
        .hasValue(registry.getAnalyzer("multiAnalyzer0"));

    assertThat(
            dynamicTypeSetPrefixMap.getNearestRoot(
                "$embedded:6/field0/$type:autocomplete/field0.field1.field2.autocomplete.field"))
        .hasValue(registry.getAutocompleteAnalyzer(AUTOCOMPLETE_FIELD_WITH_ANALYZER0));

    assertThat(
            dynamicTypeSetPrefixMap.getNearestRoot(
                "$embedded:20/field0.field1.field2/$type:token/field0.field1.field2.token.field"))
        .hasValue(registry.getNormalizer(StockNormalizerName.LOWERCASE));

    assertThat(
            dynamicTypeSetPrefixMap.getNearestRoot(
                "$embedded:13/field0.field1/$type:autocomplete/field0.field1.field2.ac.field"))
        .hasValue(registry.getAutocompleteAnalyzer(AUTOCOMPLETE_FIELD_WITH_ANALYZER1));
    assertThat(
            dynamicTypeSetPrefixMap.getNearestRoot(
                "$embedded:13/field0.field1/$type:string/field0.field1.field2.string.field"))
        .hasValue(registry.getAnalyzer("fieldAnalyzer1"));
    assertThat(
            dynamicTypeSetPrefixMap.getNearestRoot(
                "$embedded:13/field0.field1/$type:token/field0.field1.field2.token.field"))
        .hasValue(registry.getNormalizer(StockNormalizerName.LOWERCASE));

    assertThat(
            dynamicTypeSetPrefixMap.getNearestRoot(
                "$embedded:13/field0.field1/$type:token/field0.field1"))
        .isEmpty();
    assertThat(dynamicTypeSetPrefixMap.getNearestRoot("$type:token/field0")).isEmpty();
  }
}
