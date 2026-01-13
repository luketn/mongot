package com.xgen.mongot.index.analyzer.wrapper;

import static com.xgen.mongot.index.definition.SearchIndexDefinition.DEFAULT_FALLBACK_ANALYZER;
import static org.junit.Assert.assertEquals;

import com.xgen.mongot.index.analyzer.AnalyzerRegistry;
import com.xgen.mongot.index.analyzer.InvalidAnalyzerDefinitionException;
import com.xgen.mongot.index.analyzer.definition.AnalyzerDefinition;
import com.xgen.mongot.index.analyzer.definition.StockAnalyzerNames;
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
import com.xgen.testing.mongot.index.definition.TypeSetDefinitionBuilder;
import com.xgen.testing.mongot.index.path.string.StringPathBuilder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.junit.Test;

public class LuceneAnalyzerTest {

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
    return AnalyzerRegistry.factory().create(analyzers, true);
  }

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

    var documentFieldDefinitionWithDefaultingTypeset =
        FieldDefinitionBuilder.builder()
            .document(DocumentFieldDefinitionBuilder.builder().dynamic("foo").build())
            .build();

    var typeSetFoo =
        TypeSetDefinitionBuilder.builder()
            .name("foo")
            .addType(StringFieldDefinitionBuilder.builder().build())
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
            .field("document", documentFieldDefinitionWithDefaultingTypeset)
            .build();

    return SearchIndexDefinitionBuilder.builder()
        .defaultMetadata()
        .mappings(mappings)
        .typeSets(typeSetFoo)
        .analyzerName("index")
        .searchAnalyzerName("search");
  }

  @Test
  public void testQueryAnalyzerFallback() throws Exception {
    SearchIndexDefinition index = baseIndexDefinitionBuilder().build();
    QueryAnalyzerWrapper wrapper = LuceneAnalyzer.queryAnalyzer(index, getAnalyzerRegistry());

    String metaName =
        wrapper
            .getAnalyzerMeta(StringPathBuilder.fieldPath("fieldWithNoAnalyzer"), Optional.empty())
            .getName();

    assertEquals("search", metaName);
  }

  @Test
  public void testQueryAnalyzerFallbackWithTypeSet() throws Exception {
    SearchIndexDefinition index = baseIndexDefinitionBuilder().build();
    QueryAnalyzerWrapper wrapper = LuceneAnalyzer.queryAnalyzer(index, getAnalyzerRegistry());

    String metaName =
        wrapper
            .getAnalyzerMeta(StringPathBuilder.fieldPath("document.foo"), Optional.empty())
            .getName();

    assertEquals("search", metaName);
  }

  @Test
  public void testQueryAnalyzerFallbackNoSearch() throws Exception {
    SearchIndexDefinition index = baseIndexDefinitionBuilder().searchAnalyzerName(null).build();
    QueryAnalyzerWrapper wrapper = LuceneAnalyzer.queryAnalyzer(index, getAnalyzerRegistry());

    String metaName =
        wrapper
            .getAnalyzerMeta(StringPathBuilder.fieldPath("fieldWithNoAnalyzer"), Optional.empty())
            .getName();

    assertEquals("index", metaName);
  }

  @Test
  public void testQueryAnalyzerFallbackNoSearchNoIndex() throws Exception {
    SearchIndexDefinition index =
        baseIndexDefinitionBuilder().searchAnalyzerName(null).analyzerName(null).build();
    QueryAnalyzerWrapper wrapper = LuceneAnalyzer.queryAnalyzer(index, getAnalyzerRegistry());

    String metaName =
        wrapper
            .getAnalyzerMeta(StringPathBuilder.fieldPath("fieldWithNoAnalyzer"), Optional.empty())
            .getName();

    assertEquals(DEFAULT_FALLBACK_ANALYZER, metaName);
  }
}
