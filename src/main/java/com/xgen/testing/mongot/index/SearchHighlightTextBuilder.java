package com.xgen.testing.mongot.index;

import com.xgen.mongot.index.SearchHighlightText;
import com.xgen.mongot.util.Check;
import java.util.Optional;

public class SearchHighlightTextBuilder {
  private Optional<String> value = Optional.empty();
  private Optional<SearchHighlightText.Type> type = Optional.empty();

  public static SearchHighlightTextBuilder builder() {
    return new SearchHighlightTextBuilder();
  }

  public SearchHighlightTextBuilder value(String value) {
    this.value = Optional.of(value);
    return this;
  }

  public SearchHighlightTextBuilder type(SearchHighlightText.Type type) {
    this.type = Optional.of(type);
    return this;
  }

  /** Builds SearchHighlightText from an SearchHighlightTextBuilder. */
  public SearchHighlightText build() {
    Check.isPresent(this.value, "value");
    Check.isPresent(this.type, "type");

    return new SearchHighlightText(this.value.get(), this.type.get());
  }
}
