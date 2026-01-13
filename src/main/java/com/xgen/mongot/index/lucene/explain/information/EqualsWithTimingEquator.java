package com.xgen.mongot.index.lucene.explain.information;

import com.xgen.mongot.index.lucene.explain.timing.QueryExecutionArea;
import org.apache.commons.collections4.Equator;

/**
 * Explain output classes implementing this interface must utilize the supplied <code>
 * timingEquator</code> to compare any timing information they have recorded and compare the
 * remaining member variables as they would in a normal equals method.
 */
public interface EqualsWithTimingEquator<T> {
  boolean equals(T other, Equator<QueryExecutionArea> timingEquator);
}
