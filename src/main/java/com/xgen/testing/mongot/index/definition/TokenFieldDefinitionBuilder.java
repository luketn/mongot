package com.xgen.testing.mongot.index.definition;

import com.xgen.mongot.index.analyzer.definition.StockNormalizerName;
import com.xgen.mongot.index.definition.TokenFieldDefinition;
import java.util.Optional;

public class TokenFieldDefinitionBuilder {
  private Optional<StockNormalizerName> normalizer = Optional.empty();

  public static TokenFieldDefinitionBuilder builder() {
    return new TokenFieldDefinitionBuilder();
  }

  public TokenFieldDefinitionBuilder normalizerName(StockNormalizerName stockNormalizerName) {
    this.normalizer = Optional.of(stockNormalizerName);
    return this;
  }

  public TokenFieldDefinition build() {
    return new TokenFieldDefinition(this.normalizer);
  }
}
