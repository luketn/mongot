package com.xgen.mongot.index.analyzer.wrapper;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.mongot.index.definition.SearchIndexDefinition.DEFAULT_FALLBACK_ANALYZER;
import static com.xgen.mongot.index.definition.SearchIndexDefinition.DEFAULT_FALLBACK_NORMALIZER;

import com.xgen.mongot.index.analyzer.AnalyzerMeta;
import com.xgen.mongot.index.analyzer.AnalyzerRegistry;
import com.xgen.mongot.index.analyzer.InvalidAnalyzerDefinitionException;
import com.xgen.mongot.index.analyzer.definition.AnalyzerDefinition;
import com.xgen.mongot.index.analyzer.definition.NormalizerDefinition;
import com.xgen.mongot.index.analyzer.definition.StockAnalyzerNames;
import com.xgen.mongot.index.analyzer.definition.StockNormalizerName;
import com.xgen.mongot.index.definition.AutocompleteFieldDefinition;
import com.xgen.mongot.index.definition.DocumentFieldDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.testing.mongot.index.analyzer.definition.OverriddenBaseAnalyzerDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.AutocompleteFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.EmbeddedDocumentsFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.StringFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.TokenFieldDefinitionBuilder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class SearchAnalyzerBuilderTest {
  private static final AutocompleteFieldDefinition AUTOCOMPLETE_FIELD =
      AutocompleteFieldDefinitionBuilder.builder().analyzer("autocomplete").build();

  private static SearchIndexDefinitionBuilder baseIndexDefinitionBuilder() {
    var fieldWithAnalyzer =
        FieldDefinitionBuilder.builder()
            .document(DocumentFieldDefinitionBuilder.builder().build())
            .string(StringFieldDefinitionBuilder.builder().analyzerName("fieldAnalyzer").build())
            .build();

    var embeddedWithAnalyzer =
        FieldDefinitionBuilder.builder()
            .document(DocumentFieldDefinitionBuilder.builder().build())
            .embeddedDocuments(
                EmbeddedDocumentsFieldDefinitionBuilder.builder()
                    .field("nested_field", fieldWithAnalyzer)
                    .build())
            .build();

    var fieldWithSearchAnalyzer =
        FieldDefinitionBuilder.builder()
            .document(DocumentFieldDefinitionBuilder.builder().build())
            .string(
                StringFieldDefinitionBuilder.builder()
                    .analyzerName("fieldAnalyzer")
                    .searchAnalyzerName("fieldSearchAnalyzer")
                    .build())
            .build();

    var fieldWithNoAnalyzer =
        FieldDefinitionBuilder.builder()
            .document(DocumentFieldDefinitionBuilder.builder().build())
            .string(StringFieldDefinitionBuilder.builder().build())
            .build();

    var fieldWithMultiAnalyzer =
        FieldDefinitionBuilder.builder()
            .document(DocumentFieldDefinitionBuilder.builder().build())
            .string(
                StringFieldDefinitionBuilder.builder()
                    .analyzerName("fieldAnalyzer")
                    .multi(
                        "multi",
                        StringFieldDefinitionBuilder.builder()
                            .analyzerName("multiAnalyzer")
                            .build())
                    .build())
            .build();

    var autocomplete =
        FieldDefinitionBuilder.builder()
            .document(DocumentFieldDefinitionBuilder.builder().build())
            .autocomplete(AUTOCOMPLETE_FIELD)
            .build();

    var tokenWithoutNormalizer =
        FieldDefinitionBuilder.builder()
            .document(DocumentFieldDefinitionBuilder.builder().build())
            .token(TokenFieldDefinitionBuilder.builder().build())
            .build();

    var tokenWithNormalizer =
        FieldDefinitionBuilder.builder()
            .document(DocumentFieldDefinitionBuilder.builder().build())
            .token(
                TokenFieldDefinitionBuilder.builder()
                    .normalizerName(StockNormalizerName.LOWERCASE)
                    .build())
            .build();

    DocumentFieldDefinition mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(true)
            .field("fieldWithAnalyzer", fieldWithAnalyzer)
            .field("fieldWithSearchAnalyzer", fieldWithSearchAnalyzer)
            .field("fieldWithMultiAnalyzer", fieldWithMultiAnalyzer)
            .field("embeddedDoc", embeddedWithAnalyzer)
            .field("fieldWithNoAnalyzer", fieldWithNoAnalyzer)
            .field("autocompleteField", autocomplete)
            .field("tokenWithoutNormalizer", tokenWithoutNormalizer)
            .field("tokenWithNormalizer", tokenWithNormalizer)
            .build();

    return SearchIndexDefinitionBuilder.builder()
        .defaultMetadata()
        .mappings(mappings)
        .analyzerName("index")
        .searchAnalyzerName("search");
  }

  private static AnalyzerRegistry getAnalyzerRegistry() throws InvalidAnalyzerDefinitionException {
    List<AnalyzerDefinition> analyzers = new ArrayList<>();
    for (String name :
        Arrays.asList(
            "search",
            "index",
            "fieldAnalyzer",
            "fieldSearchAnalyzer",
            "multiAnalyzer",
            "autocomplete")) {
      analyzers.add(
          OverriddenBaseAnalyzerDefinitionBuilder.builder()
              .baseAnalyzerName(StockAnalyzerNames.LUCENE_STANDARD.getName())
              .name(name)
              .build());
    }

    analyzers.add(NormalizerDefinition.stockNormalizer(StockNormalizerName.LOWERCASE));
    return AnalyzerRegistry.factory().create(analyzers, true);
  }

  @Test
  public void testBuild() throws Exception {
    AnalyzerRegistry registry = getAnalyzerRegistry();
    AnalyzerMeta fallback = registry.getAnalyzerMeta(DEFAULT_FALLBACK_ANALYZER);
    AnalyzerMeta fallbackNormalizer = registry.getNormalizerMeta(DEFAULT_FALLBACK_NORMALIZER);
    SearchIndexDefinition index = baseIndexDefinitionBuilder().build();
    AnalyzerMeta fieldAnalyzer = registry.getAnalyzerMeta("fieldAnalyzer");
    AnalyzerMeta fieldSearchAnalyzer = registry.getAnalyzerMeta("fieldSearchAnalyzer");
    Map<String, AnalyzerMeta> expected =
        Map.of(
            "$type:string/fieldWithMultiAnalyzer",
            fieldAnalyzer,
            "$multi/fieldWithMultiAnalyzer.multi",
            registry.getAnalyzerMeta("multiAnalyzer"),
            "$type:string/fieldWithAnalyzer",
            fieldAnalyzer,
            "$type:string/fieldWithSearchAnalyzer",
            fieldSearchAnalyzer,
            "$type:string/fieldWithNoAnalyzer",
            registry.getAnalyzerMeta(DEFAULT_FALLBACK_ANALYZER),
            "$embedded:11/embeddedDoc/$type:string/embeddedDoc.nested_field",
            fieldAnalyzer,
            "$type:autocomplete/autocompleteField",
            registry.getAnalyzerMeta("autocomplete"),
            "$type:token/tokenWithoutNormalizer",
            fallbackNormalizer,
            "$type:token/tokenWithNormalizer",
            registry.getNormalizerMeta(StockNormalizerName.LOWERCASE));

    Map<String, AnalyzerMeta> result =
        new QueryAnalyzerBuilder()
            .buildStaticMappings(index, registry, fallback, fallbackNormalizer);

    assertThat(result).containsExactlyEntriesIn(expected);
  }
}
