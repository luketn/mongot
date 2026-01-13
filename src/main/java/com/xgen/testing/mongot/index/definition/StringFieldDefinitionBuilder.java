package com.xgen.testing.mongot.index.definition;

import com.xgen.mongot.index.definition.SimilarityDefinition;
import com.xgen.mongot.index.definition.StringFieldDefinition;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class StringFieldDefinitionBuilder {

  private Optional<String> analyzerName = Optional.empty();
  private Optional<String> searchAnalyzerName = Optional.empty();
  private Optional<SimilarityDefinition> similarity = Optional.empty();
  private Optional<Integer> ignoreAbove = Optional.empty();
  private Optional<StringFieldDefinition.IndexOptions> indexOptions = Optional.empty();
  private Optional<Boolean> store = Optional.empty();
  private Optional<StringFieldDefinition.NormsOptions> norms = Optional.empty();
  private final Map<String, StringFieldDefinition> multi = new HashMap<>();

  public static StringFieldDefinitionBuilder builder() {
    return new StringFieldDefinitionBuilder();
  }

  public StringFieldDefinitionBuilder analyzerName(String analyzer) {
    this.analyzerName = Optional.of(analyzer);
    return this;
  }

  public StringFieldDefinitionBuilder searchAnalyzerName(String analyzer) {
    this.searchAnalyzerName = Optional.of(analyzer);
    return this;
  }

  public StringFieldDefinitionBuilder similarity(SimilarityDefinition similarity) {
    this.similarity = Optional.of(similarity);
    return this;
  }

  public StringFieldDefinitionBuilder ignoreAbove(int ignoreAbove) {
    this.ignoreAbove = Optional.of(ignoreAbove);
    return this;
  }

  public StringFieldDefinitionBuilder indexOptions(
      StringFieldDefinition.IndexOptions indexOptions) {
    this.indexOptions = Optional.of(indexOptions);
    return this;
  }

  public StringFieldDefinitionBuilder store(boolean store) {
    this.store = Optional.of(store);
    return this;
  }

  public StringFieldDefinitionBuilder norms(StringFieldDefinition.NormsOptions norms) {
    this.norms = Optional.of(norms);
    return this;
  }

  public StringFieldDefinitionBuilder multi(String name, StringFieldDefinition definition) {
    this.multi.put(name, definition);
    return this;
  }

  public StringFieldDefinition build() {
    return StringFieldDefinition.create(
        this.analyzerName,
        this.searchAnalyzerName,
        this.similarity,
        this.ignoreAbove,
        this.indexOptions.orElse(StringFieldDefinition.Fields.INDEX_OPTIONS.getDefaultValue()),
        this.store.orElse(StringFieldDefinition.Fields.STORE.getDefaultValue()),
        this.norms.orElse(StringFieldDefinition.Fields.NORMS.getDefaultValue()),
        this.multi);
  }
}
