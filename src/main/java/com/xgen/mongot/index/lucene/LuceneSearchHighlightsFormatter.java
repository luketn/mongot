package com.xgen.mongot.index.lucene;

import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import com.xgen.mongot.index.SearchHighlightText;
import com.xgen.mongot.util.Check;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.lucene.search.uhighlight.Passage;
import org.apache.lucene.search.uhighlight.PassageFormatter;

/**
 * Creates a json formatted searchHighlight from the top passages.
 *
 * <p>The default implementation marks the query terms as bold, and places ellipses between
 * unconnected passages.
 */
class LuceneSearchHighlightsFormatter extends PassageFormatter {

  LuceneSearchHighlightsFormatter() {}

  /**
   * Take an array of org.apache.lucene.search.uhighlight.Passage and return the data as a
   * SearchHighlights object.
   */
  @Override
  public Object format(Passage[] passages, String content) {
    return new LuceneSearchHighlights(
        Stream.of(passages)
            .filter(passage -> passage.getNumMatches() > 0)
            .map(
                passage ->
                    new LuceneSearchHighlight(passage.getScore(), getHighlights(passage, content)))
            .collect(Collectors.toList()));
  }

  static class PassageText {
    public final String text;
    public final Range<Integer> range;
    public final SearchHighlightText.Type hitType;

    public PassageText(String text, Range<Integer> range, SearchHighlightText.Type hitType) {
      this.text = text;
      this.range = range;
      this.hitType = hitType;
    }
  }

  /** Get Highlights. */
  public static List<SearchHighlightText> getHighlights(Passage passage, String content) {

    validatePassageContent(passage, content);

    // get all the hits
    List<Range<Integer>> ranges =
        IntStream.range(0, passage.getNumMatches())
            .mapToObj(
                i ->
                    Range.closedOpen(passage.getMatchStarts()[i], passage.getMatchEnds()[i])
                        .canonical(DiscreteDomain.integers()))
            .collect(Collectors.toList());

    RangeSet<Integer> rs = TreeRangeSet.create(ranges);

    // contiguous ranges of hits
    Set<Range<Integer>> hits = rs.asRanges();

    // any texts
    Set<Range<Integer>> texts =
        rs.complement()
            .subRangeSet(Range.closedOpen(passage.getStartOffset(), passage.getEndOffset()))
            .asRanges();

    Stream<PassageText> passageHits =
        hits.stream()
            .map(
                h ->
                    new PassageText(
                        content.substring(h.lowerEndpoint(), h.upperEndpoint()),
                        h,
                        SearchHighlightText.Type.HIT));

    Stream<PassageText> passageTexts =
        texts.stream()
            .map(
                h ->
                    new PassageText(
                        content.substring(h.lowerEndpoint(), h.upperEndpoint()),
                        h,
                        SearchHighlightText.Type.TEXT));

    // sort by lower value and return as Search Highlights
    return Stream.concat(passageHits, passageTexts)
        .sorted(Comparator.comparing(p -> p.range.lowerEndpoint()))
        .map(p -> new SearchHighlightText(p.text, p.hitType))
        .collect(Collectors.toList());
  }

  /** Validate the Passage and content String. */
  public static void validatePassageContent(Passage passage, String content) {
    Check.argNotEmpty(content, "content");

    if (passage.getStartOffset() < 0) {
      throw new IllegalStateException("startOffset must be greater than 0");
    }

    if (passage.getEndOffset() <= passage.getStartOffset()) {
      throw new IllegalStateException("endOffset must be greater than startOffset");
    }

    if (passage.getEndOffset() > content.length()) {
      throw new IllegalStateException("endOffset must be <= content.length");
    }

    if (passage.getNumMatches() < 1) {
      throw new IllegalStateException("must have at least one match");
    }

    if (IntStream.range(0, passage.getNumMatches())
        .mapToObj(i -> (passage.getMatchStarts()[i] >= passage.getMatchEnds()[i]))
        .anyMatch(test -> test == true)) {
      throw new IllegalStateException("matchEnds must be greater than matchStarts.");
    }
  }
}
