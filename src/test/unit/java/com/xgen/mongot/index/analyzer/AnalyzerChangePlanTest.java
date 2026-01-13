package com.xgen.mongot.index.analyzer;

import com.xgen.mongot.index.analyzer.definition.AnalyzerDefinition;
import com.xgen.mongot.index.analyzer.definition.OverriddenBaseAnalyzerDefinition;
import com.xgen.mongot.index.analyzer.definition.StockAnalyzerNames;
import com.xgen.testing.mongot.index.analyzer.definition.OverriddenBaseAnalyzerDefinitionBuilder;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class AnalyzerChangePlanTest {
  @Test
  public void testNoEmptyChanges() {
    var modified =
        AnalyzerChangePlan.modifiedOverridenAnalyzers(
            Collections.emptyList(), Collections.emptyList());
    Assert.assertEquals(0, modified.size());
  }

  @Test
  public void testNoChangeToAnalyzer() {
    var definition =
        OverriddenBaseAnalyzerDefinitionBuilder.builder()
            .name("analyzer1")
            .baseAnalyzerName(StockAnalyzerNames.LUCENE_GALICIAN.getName())
            .build();

    var modified =
        AnalyzerChangePlan.modifiedOverridenAnalyzers(List.of(definition), List.of(definition));
    Assert.assertEquals(0, modified.size());
  }

  @Test
  public void testAnalyzerRemoved() {
    var definition =
        OverriddenBaseAnalyzerDefinitionBuilder.builder()
            .name("analyzer1")
            .baseAnalyzerName(StockAnalyzerNames.LUCENE_GALICIAN.getName())
            .build();

    var modified =
        AnalyzerChangePlan.modifiedOverridenAnalyzers(List.of(definition), Collections.emptyList());
    Assert.assertEquals(0, modified.size());
  }

  @Test
  public void testAnalyzerAdded() {
    var definition =
        OverriddenBaseAnalyzerDefinitionBuilder.builder()
            .name("analyzer1")
            .baseAnalyzerName(StockAnalyzerNames.LUCENE_GALICIAN.getName())
            .build();

    var modified =
        AnalyzerChangePlan.modifiedOverridenAnalyzers(Collections.emptyList(), List.of(definition));
    Assert.assertEquals(0, modified.size());
  }

  @Test
  public void testAnalyzerModified() {
    var definitionV1 =
        OverriddenBaseAnalyzerDefinitionBuilder.builder()
            .name("analyzer1")
            .baseAnalyzerName(StockAnalyzerNames.LUCENE_GALICIAN.getName())
            .build();

    var definitionV2 =
        OverriddenBaseAnalyzerDefinitionBuilder.builder()
            .name("analyzer1")
            .baseAnalyzerName(StockAnalyzerNames.LUCENE_STANDARD.getName())
            .build();

    var modified =
        AnalyzerChangePlan.modifiedOverridenAnalyzers(List.of(definitionV1), List.of(definitionV2));
    Assert.assertEquals(1, modified.size());
  }

  @Test
  public void testFindChangesMultipleAnalyzers() {
    OverriddenBaseAnalyzerDefinition definition1v1 =
        OverriddenBaseAnalyzerDefinitionBuilder.builder()
            .name("analyzer1")
            .baseAnalyzerName(StockAnalyzerNames.LUCENE_GALICIAN.getName())
            .stopword("one")
            .stopword("two")
            .stopword("three")
            .excludeStem("four")
            .excludeStem("five")
            .excludeStem("six")
            .build();

    OverriddenBaseAnalyzerDefinition definition1v2 =
        OverriddenBaseAnalyzerDefinitionBuilder.builder()
            .name("analyzer1")
            .baseAnalyzerName(StockAnalyzerNames.LUCENE_GALICIAN.getName())
            .stopword("three")
            .stopword("two")
            .stopword("one")
            .excludeStem("six")
            .excludeStem("five")
            .excludeStem("four")
            .ignoreCase(true)
            .build();

    OverriddenBaseAnalyzerDefinition definition2v1 =
        OverriddenBaseAnalyzerDefinitionBuilder.builder()
            .name("analyzer2")
            .baseAnalyzerName(StockAnalyzerNames.LUCENE_ITALIAN.getName())
            .build();

    OverriddenBaseAnalyzerDefinition definition2v2 =
        OverriddenBaseAnalyzerDefinitionBuilder.builder()
            .name("analyzer2")
            .baseAnalyzerName(StockAnalyzerNames.LUCENE_ITALIAN.getName())
            .stopword("one")
            .build();

    OverriddenBaseAnalyzerDefinition definition3v1 =
        OverriddenBaseAnalyzerDefinitionBuilder.builder()
            .name("analyzer3")
            .baseAnalyzerName(StockAnalyzerNames.LUCENE_STANDARD.getName())
            .maxTokenLength(55)
            .build();

    OverriddenBaseAnalyzerDefinition definition3v2 =
        OverriddenBaseAnalyzerDefinitionBuilder.builder()
            .name("analyzer3")
            .baseAnalyzerName(StockAnalyzerNames.LUCENE_STANDARD.getName())
            .maxTokenLength(25)
            .build();

    OverriddenBaseAnalyzerDefinition definition4 =
        OverriddenBaseAnalyzerDefinitionBuilder.builder()
            .name("analyzer4")
            .baseAnalyzerName(StockAnalyzerNames.LUCENE_STOP.getName())
            .stopword("one")
            .build();

    OverriddenBaseAnalyzerDefinition definition5 =
        OverriddenBaseAnalyzerDefinitionBuilder.builder()
            .name("analyzer5")
            .baseAnalyzerName(StockAnalyzerNames.LUCENE_STOP.getName())
            .stopword("one")
            .build();

    OverriddenBaseAnalyzerDefinition definition6 =
        OverriddenBaseAnalyzerDefinitionBuilder.builder()
            .name("analyzer6")
            .baseAnalyzerName(StockAnalyzerNames.LUCENE_STOP.getName())
            .stopword("one")
            .build();

    Assert.assertEquals(definition1v1, definition1v2);
    var existingDefinitions =
        List.of(definition1v1, definition2v1, definition3v1, definition4, definition5);

    // Create a list of desired definitions that has a different but equivalent definition for
    // definition 1, changes definitions 2, and 3, removes 4, keeps
    // 5 the same, and adds 6.
    List<OverriddenBaseAnalyzerDefinition> desiredDefinitions =
        Arrays.asList(definition1v2, definition2v2, definition3v2, definition5, definition6);

    var modified =
        AnalyzerChangePlan.modifiedOverridenAnalyzers(existingDefinitions, desiredDefinitions);

    Map<String, AnalyzerDefinition> expectedModified = new HashMap<>();
    expectedModified.put("analyzer2", definition2v2);
    expectedModified.put("analyzer3", definition3v2);
    Assert.assertEquals(expectedModified, modified);
  }
}
