package com.xgen.mongot.index.lucene.similarity;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;

import com.xgen.mongot.index.definition.DocumentFieldDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.SimilarityDefinition;
import com.xgen.testing.mongot.index.definition.AutocompleteFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.EmbeddedDocumentsFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.StringFieldDefinitionBuilder;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.BooleanSimilarity;
import org.apache.lucene.search.similarities.PerFieldSimilarityWrapper;
import org.apache.lucene.search.similarities.Similarity;
import org.junit.Test;

public class LuceneSimilarityTest {

  private static SearchIndexDefinition indexDefinitionWithNoSimilarity() {
    var stringFieldWithNoSimilarity =
        FieldDefinitionBuilder.builder()
            .document(DocumentFieldDefinitionBuilder.builder().build())
            .string(StringFieldDefinitionBuilder.builder().build())
            .build();

    DocumentFieldDefinition mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(true)
            .field("stringFieldWithNoSimilarity", stringFieldWithNoSimilarity)
            .build();

    return SearchIndexDefinitionBuilder.builder().defaultMetadata().mappings(mappings).build();
  }

  private static SearchIndexDefinition indexDefinitionWithSimilarityPerField() {
    var stringFieldWithNoSimilarity =
        FieldDefinitionBuilder.builder()
            .document(DocumentFieldDefinitionBuilder.builder().build())
            .string(StringFieldDefinitionBuilder.builder().build())
            .build();

    var stringFieldWithBoolean =
        FieldDefinitionBuilder.builder()
            .document(DocumentFieldDefinitionBuilder.builder().build())
            .string(
                StringFieldDefinitionBuilder.builder()
                    .similarity(SimilarityDefinition.BOOLEAN)
                    .build())
            .build();

    var autocompleteFieldWithBM25 =
        FieldDefinitionBuilder.builder()
            .document(DocumentFieldDefinitionBuilder.builder().build())
            .autocomplete(
                AutocompleteFieldDefinitionBuilder.builder()
                    .similarity(SimilarityDefinition.BM25)
                    .build())
            .build();

    var autocompleteFieldWithTflTuned =
        FieldDefinitionBuilder.builder()
            .document(DocumentFieldDefinitionBuilder.builder().build())
            .autocomplete(
                AutocompleteFieldDefinitionBuilder.builder()
                    .similarity(SimilarityDefinition.STABLE_TFL)
                    .build())
            .build();

    var embeddedFieldWithSimilarity =
        FieldDefinitionBuilder.builder()
            .document(DocumentFieldDefinitionBuilder.builder().build())
            .embeddedDocuments(
                EmbeddedDocumentsFieldDefinitionBuilder.builder()
                    .field("string_field_with_boolean", stringFieldWithBoolean)
                    .field("autocomplete_field_with_TFL_tuned", autocompleteFieldWithTflTuned)
                    .build())
            .build();

    var stringFieldWithMultiSimilarity =
        FieldDefinitionBuilder.builder()
            .document(DocumentFieldDefinitionBuilder.builder().build())
            .string(
                StringFieldDefinitionBuilder.builder()
                    .similarity(SimilarityDefinition.BOOLEAN)
                    .multi(
                        "multi",
                        StringFieldDefinitionBuilder.builder()
                            .similarity(SimilarityDefinition.STABLE_TFL)
                            .build())
                    .build())
            .build();

    var childField =
        FieldDefinitionBuilder.builder()
            .document(
                DocumentFieldDefinitionBuilder.builder()
                    .field(
                        "child",
                        FieldDefinitionBuilder.builder()
                            .string(
                                StringFieldDefinitionBuilder.builder()
                                    .similarity(SimilarityDefinition.STABLE_TFL)
                                    .build())
                            .build())
                    .build())
            .build();

    DocumentFieldDefinition mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(true)
            .field("stringFieldWithNoSimilarity", stringFieldWithNoSimilarity)
            .field("stringFieldWithBoolean", stringFieldWithBoolean)
            .field("autocompleteFieldWitBM25", autocompleteFieldWithBM25)
            .field("autocompleteFieldWitTflTuned", autocompleteFieldWithTflTuned)
            .field("embeddedFieldWithSimilarity", embeddedFieldWithSimilarity)
            .field("stringFieldWithMultiSimilarity", stringFieldWithMultiSimilarity)
            .field("parent.child", stringFieldWithBoolean)
            .field("parent", childField)
            .build();

    return SearchIndexDefinitionBuilder.builder().defaultMetadata().mappings(mappings).build();
  }

  @Test
  public void factory_noSimilarity_returnsDefault() {
    SearchIndexDefinition index = indexDefinitionWithNoSimilarity();
    Similarity similarity = LuceneSimilarity.from(index);
    assertThat(similarity).isInstanceOf(BM25Similarity.class);
  }

  @Test
  public void factory_fromSearchIndex_returnsSimilarityPerField() {
    SearchIndexDefinition index = indexDefinitionWithSimilarityPerField();
    Similarity similarity = LuceneSimilarity.from(index);
    assertThat(similarity).isInstanceOf(PerFieldSimilarityWrapper.class);
    var wrapper = (PerFieldSimilarityWrapper) similarity;

    assertThat(wrapper.get("$type:string/stringFieldWithNoSimilarity"))
        .isInstanceOf(BM25Similarity.class);
    assertThat(wrapper.get("$type:string/stringFieldWithBoolean"))
        .isInstanceOf(BooleanSimilarity.class);
    assertThat(wrapper.get("$type:string/stringFieldWithMultiSimilarity"))
        .isInstanceOf(BooleanSimilarity.class);
    assertThat(wrapper.get("$type:string/stringFieldWithNoSimilarity"))
        .isInstanceOf(BM25Similarity.class);
    assertThat(wrapper.get("$type:string/stringFieldWithNoSimilarity"))
        .isInstanceOf(BM25Similarity.class);
    assertThat(wrapper.get("$type:string/stringFieldWithNoSimilarity"))
        .isInstanceOf(BM25Similarity.class);

    Similarity multiTfl = wrapper.get("$multi/stringFieldWithMultiSimilarity.multi");
    assertThat(multiTfl).isInstanceOf(StableTflSimilarity.class);
    assertEquals("StableTFLSimilarity(k1=1.2, c=0.917, m=0.00781)", multiTfl.toString());

    assertThat(wrapper.get("$type:autocomplete/autocompleteFieldWitBM25"))
        .isInstanceOf(BM25Similarity.class);
    Similarity autocompleteTfl = wrapper.get("$type:autocomplete/autocompleteFieldWitTflTuned");
    assertThat(autocompleteTfl).isInstanceOf(StableTflSimilarity.class);
    assertEquals("StableTFLSimilarity(k1=1.2, c=0.917, m=0.00781)", autocompleteTfl.toString());

    assertThat(
            wrapper.get(
                "$embedded:27/embeddedFieldWithSimilarity/$type:string/"
                    + "embeddedFieldWithSimilarity.string_field_with_boolean"))
        .isInstanceOf(BooleanSimilarity.class);
    Similarity embeddedTfl =
        wrapper.get(
            "$embedded:27/embeddedFieldWithSimilarity/$type:autocomplete/"
                + "embeddedFieldWithSimilarity.autocomplete_field_with_TFL_tuned");
    assertThat(embeddedTfl).isInstanceOf(StableTflSimilarity.class);
    assertEquals("StableTFLSimilarity(k1=1.2, c=0.917, m=0.00781)", embeddedTfl.toString());
  }

  @Test
  public void factory_conflictingTypes_resolvesDeterministically() {
    SearchIndexDefinition index = indexDefinitionWithSimilarityPerField();
    Similarity similarity = LuceneSimilarity.from(index);
    assertThat(similarity).isInstanceOf(PerFieldSimilarityWrapper.class);
    var wrapper = (PerFieldSimilarityWrapper) similarity;

    // Implementation gives preference to definition of dotted path fields over nested fields
    assertThat(wrapper.get("$type:string/parent.child")).isInstanceOf(BooleanSimilarity.class);
  }
}
