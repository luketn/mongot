package com.xgen.mongot.index.lucene.query;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.lucene.synonym.LuceneSynonymMapBuilder;
import com.xgen.mongot.index.synonym.SynonymDocument;
import com.xgen.mongot.index.synonym.SynonymMapping;
import com.xgen.mongot.index.synonym.SynonymMappingException;
import com.xgen.mongot.index.synonym.SynonymRegistry;
import com.xgen.testing.mongot.index.analyzer.AnalyzerRegistryBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

public class SynonymTestUtil {
  static LuceneSearchTranslation synonymsTest(
      List<String> equivalentSynonyms, String synonymMappingName) throws SynonymMappingException {
    SearchIndexDefinition indexDefinition = SearchIndexDefinitionBuilder.VALID_INDEX;
    SynonymRegistry synonymRegistry = mock(SynonymRegistry.class);
    SynonymMapping synonymMapping =
        LuceneSynonymMapBuilder.builder(new StandardAnalyzer(), "lucene.standard")
            .addDocument(
                SynonymDocument.create(
                    SynonymDocument.MappingType.EQUIVALENT,
                    equivalentSynonyms,
                    Optional.empty(),
                    Optional.empty()))
            .build();

    when(synonymRegistry.get(synonymMappingName)).thenReturn(synonymMapping);
    return LuceneSearchTranslation.synonyms(
        AnalyzerRegistryBuilder.empty(), indexDefinition, synonymRegistry);
  }
}
