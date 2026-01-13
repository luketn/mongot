package com.xgen.mongot.index.lucene;

import com.xgen.mongot.index.SearchHighlight;
import com.xgen.mongot.index.path.string.StringPath;
import java.util.ArrayList;
import java.util.List;

class LuceneSearchHighlights {

  private final List<LuceneSearchHighlight> luceneSearchHighlights;

  LuceneSearchHighlights(List<LuceneSearchHighlight> searchHighlights) {
    this.luceneSearchHighlights = searchHighlights;
  }

  /** Convert this to a list of SearchHighlight objects. */
  List<SearchHighlight> toSearchHighlights(StringPath path) {
    List<SearchHighlight> searchHighlights = new ArrayList<>();
    this.luceneSearchHighlights.forEach(
        luceneSearchHighlight ->
            searchHighlights.add(luceneSearchHighlight.toSearchHighlight(path)));
    return searchHighlights;
  }
}
