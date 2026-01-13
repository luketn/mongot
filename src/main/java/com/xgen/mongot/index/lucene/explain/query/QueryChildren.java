package com.xgen.mongot.index.lucene.explain.query;

import com.google.errorprone.annotations.Var;
import java.util.Collection;
import java.util.Optional;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;

public interface QueryChildren<T extends QueryExecutionContextNode> {

  Collection<T> must();

  Collection<T> mustNot();

  Collection<T> should();

  Collection<T> filter();

  void addClause(T child, BooleanClause.Occur occur);

  /** Find the occur for a specific child. */
  Optional<Occur> occurFor(T child);

  void removeChild(T child);

  default String toString(
      Collection<T> must, Collection<T> mustNot, Collection<T> should, Collection<T> filter) {
    var sb = new StringBuilder();
    @Var boolean hasOtherElement = false;
    if (!must.isEmpty()) {
      sb.append(String.format("must(%s)", must));
      hasOtherElement = true;
    }
    if (!mustNot.isEmpty()) {
      if (hasOtherElement) {
        sb.append(", ");
      }
      sb.append(String.format("mustNot(%s)", mustNot));
      hasOtherElement = true;
    }
    if (!should.isEmpty()) {
      if (hasOtherElement) {
        sb.append(", ");
      }
      sb.append(String.format("should(%s)", should));
      hasOtherElement = true;
    }
    if (!filter.isEmpty()) {
      if (hasOtherElement) {
        sb.append(", ");
      }
      sb.append(String.format("filter(%s)", filter));
    }
    return sb.toString();
  }
}
