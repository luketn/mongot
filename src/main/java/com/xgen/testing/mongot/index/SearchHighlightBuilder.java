package com.xgen.testing.mongot.index;

import com.xgen.mongot.index.SearchHighlight;
import com.xgen.mongot.index.SearchHighlightText;
import com.xgen.mongot.index.path.string.StringPath;
import com.xgen.mongot.util.Check;
import java.util.List;
import java.util.Optional;

public class SearchHighlightBuilder {
  private Optional<Float> score = Optional.empty();

  private Optional<StringPath> path = Optional.empty();
  private Optional<List<SearchHighlightText>> texts = Optional.empty();

  public static SearchHighlightBuilder builder() {
    return new SearchHighlightBuilder();
  }

  public SearchHighlightBuilder score(Float score) {
    this.score = Optional.of(score);
    return this;
  }

  public SearchHighlightBuilder path(StringPath path) {
    this.path = Optional.of(path);
    return this;
  }

  public SearchHighlightBuilder texts(List<SearchHighlightText> texts) {
    this.texts = Optional.of(texts);
    return this;
  }

  /** Builds SearchHighlight from an SearchHighlightBuilder. */
  public SearchHighlight build() {
    Check.isPresent(this.score, "score");

    Check.isPresent(this.path, "path");
    Check.isPresent(this.texts, "texts");

    return new SearchHighlight(this.score.get(), this.path.get(), this.texts.get());
  }
}
