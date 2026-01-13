package com.xgen.mongot.index.lucene.query;

import com.xgen.mongot.index.lucene.query.util.LucenePath;
import com.xgen.mongot.index.path.string.StringPath;
import com.xgen.mongot.index.path.string.UnresolvedStringPath;
import com.xgen.mongot.util.FieldPath;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.lucene.index.IndexReader;

class StringPathQuery {

  private final String query;
  private final StringPath path;

  /** All pairs of paths and queries. */
  static Stream<StringPathQuery> resolveAndProduct(
      IndexReader indexReader,
      Optional<FieldPath> embeddedRoot,
      List<UnresolvedStringPath> path,
      List<String> query) {
    List<StringPath> stringPaths = LucenePath.resolveStringPaths(indexReader, path, embeddedRoot);
    return product(stringPaths, query);
  }

  static Stream<StringPathQuery> resolvedProduct(List<StringPath> paths, List<String> query) {
    return product(paths, query);
  }

  /** All pairs of paths and queries. */
  static Stream<StringPathQuery> product(Collection<StringPath> paths, Collection<String> queries) {
    return queries.stream().flatMap(q -> paths.stream().map(p -> new StringPathQuery(q, p)));
  }

  StringPathQuery(String query, StringPath path) {
    this.path = path;
    this.query = query;
  }

  public StringPath getPath() {
    return this.path;
  }

  public String getQuery() {
    return this.query;
  }
}
