package com.xgen.mongot.index.lucene;

import com.xgen.mongot.index.SearchHighlight;
import com.xgen.mongot.index.SearchHighlightText;
import com.xgen.mongot.index.path.string.StringPath;
import java.util.List;

class LuceneSearchHighlight {

  private final List<SearchHighlightText> texts;
  private final float score;

  LuceneSearchHighlight(float score, List<SearchHighlightText> searchHighlightTexts) {
    if (Float.isNaN(score)) {
      throw new IllegalArgumentException("score is NaN");
    }
    this.score = score;
    this.texts = searchHighlightTexts;
  }

  SearchHighlight toSearchHighlight(StringPath path) {
    return new SearchHighlight(this.score, path, this.texts);
  }
}
