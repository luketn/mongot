package com.xgen.mongot.index.analyzer;

import com.xgen.mongot.index.analyzer.definition.AnalyzerDefinition;
import com.xgen.mongot.index.analyzer.definition.OverriddenBaseAnalyzerDefinition;
import com.xgen.mongot.index.analyzer.definition.StockAnalyzerNames;
import java.util.Objects;
import org.apache.lucene.analysis.Analyzer;

public class AnalyzerMeta {

  private final AnalyzerDefinition analyzerDefinition;
  private final Analyzer analyzer;

  AnalyzerMeta(Analyzer analyzer, AnalyzerDefinition analyzerDefinition) {
    this.analyzer = analyzer;
    this.analyzerDefinition = analyzerDefinition;
  }

  public Analyzer getAnalyzer() {
    return this.analyzer;
  }

  /** Whether or not this analyzer is a keyword analyzer, or is derived from one. */
  public boolean derivedFromKeyword() {
    // TODO(CLOUDP-58217): this information should come from AnalyzerProvider-s, and not "guessed"
    // by name
    if (this.analyzerDefinition
        instanceof OverriddenBaseAnalyzerDefinition overriddenBaseAnalyzerDefinition) {
      return StockAnalyzerNames.LUCENE_KEYWORD
          .getName()
          .equals(overriddenBaseAnalyzerDefinition.getBaseAnalyzerName());
    } else {
      return false;
    }
  }

  /**
   * Returns the name which would correspond to {@link #getAnalyzer()} in the {@link
   * AnalyzerRegistry}.
   */
  public String getName() {
    return this.analyzerDefinition.name();
  }

  @Override
  public String toString() {
    return "AnalyzerMeta(name=" + getName() + ", analyzer=" + this.analyzer + ")";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AnalyzerMeta)) {
      return false;
    }
    AnalyzerMeta that = (AnalyzerMeta) o;
    return this.analyzerDefinition.equals(that.analyzerDefinition)
        && this.analyzer.equals(that.analyzer);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.analyzerDefinition, this.analyzer);
  }
}
