package com.xgen.testing.mongot.index.query.highlights;

import com.xgen.mongot.index.path.string.UnresolvedStringPath;
import com.xgen.mongot.index.query.highlights.UnresolvedHighlight;
import com.xgen.testing.mongot.index.path.string.UnresolvedStringPathBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class UnresolvedHighlightBuilder {

  private final List<UnresolvedStringPath> paths = new ArrayList<>();
  private Optional<Integer> maxNumPassages = Optional.empty();
  private Optional<Integer> maxCharsToExamine = Optional.empty();

  public static UnresolvedHighlightBuilder builder() {
    return new UnresolvedHighlightBuilder();
  }

  public UnresolvedHighlightBuilder path(UnresolvedStringPath path) {
    this.paths.add(path);
    return this;
  }

  public UnresolvedHighlightBuilder path(String path) {
    this.paths.add(UnresolvedStringPathBuilder.fieldPath(path));
    return this;
  }

  public UnresolvedHighlightBuilder multi(String path, String multi) {
    this.paths.add(UnresolvedStringPathBuilder.withMulti(path, multi));
    return this;
  }

  public UnresolvedHighlightBuilder maxNumPassages(int maxNumPassages) {
    this.maxNumPassages = Optional.of(maxNumPassages);
    return this;
  }

  public UnresolvedHighlightBuilder maxCharsToExamine(int maxCharsToExamine) {
    this.maxCharsToExamine = Optional.of(maxCharsToExamine);
    return this;
  }

  /** Build the UnresolvedHighlight. */
  public UnresolvedHighlight build() {
    return new UnresolvedHighlight(
        this.paths,
        this.maxNumPassages.orElse(UnresolvedHighlight.Fields.MAX_NUM_PASSAGES.getDefaultValue()),
        this.maxCharsToExamine.orElse(
            UnresolvedHighlight.Fields.MAX_CHARS_TO_EXAMINE.getDefaultValue()));
  }
}
