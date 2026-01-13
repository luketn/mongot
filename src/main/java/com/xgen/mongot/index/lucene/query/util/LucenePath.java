package com.xgen.mongot.index.lucene.query.util;

import com.google.common.base.Splitter;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.path.string.StringFieldPath;
import com.xgen.mongot.index.path.string.StringMultiFieldPath;
import com.xgen.mongot.index.path.string.StringPath;
import com.xgen.mongot.index.path.string.UnresolvedStringFieldPath;
import com.xgen.mongot.index.path.string.UnresolvedStringMultiFieldPath;
import com.xgen.mongot.index.path.string.UnresolvedStringPath;
import com.xgen.mongot.index.path.string.UnresolvedStringWildcardPath;
import com.xgen.mongot.util.FieldPath;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexReader;

public class LucenePath {

  private static final String WILDCARD_REGEX = ".*?";

  // Use Splitter here because, unlike String.split(regex), it is optimized for literal char match,
  // avoids allocating temporary lists, and avoids inconsistent handling of trailing matches.
  private static final Splitter STAR_SPLITTER = Splitter.on('*');

  /**
   * Returns a list of all {@link StringPath}s in the index that match at least one {@link
   * UnresolvedStringPath}.
   */
  public static List<StringPath> resolveStringPaths(
      IndexReader indexReader, List<UnresolvedStringPath> paths, Optional<FieldPath> embeddedRoot) {
    List<String> processedLucenePaths = getPathsInLucene(indexReader, embeddedRoot);
    return paths.stream()
        .flatMap(path -> resolveStringPath(processedLucenePaths, path))
        .distinct()
        .collect(Collectors.toList());
  }

  /** Resolves only wildcard paths. */
  public static Stream<StringPath> resolveWildcardPaths(
      IndexReader indexReader,
      List<UnresolvedStringWildcardPath> paths,
      Optional<FieldPath> embeddedRoot) {
    List<String> processedLucenePaths = getPathsInLucene(indexReader, embeddedRoot);
    return paths.stream()
        .flatMap(path -> resolveWildcardPath(processedLucenePaths, path))
        .distinct();
  }

  private static Stream<StringPath> resolveStringPath(
      List<String> processedLucenePaths, UnresolvedStringPath path) {
    return switch (path) {
      case UnresolvedStringFieldPath fieldPath ->
          Stream.of(new StringFieldPath(fieldPath.getValue()));
      case UnresolvedStringMultiFieldPath multiFieldPath ->
          Stream.of(
              new StringMultiFieldPath(multiFieldPath.getFieldPath(), multiFieldPath.getMulti()));
      case UnresolvedStringWildcardPath wildcardPath ->
          resolveWildcardPath(processedLucenePaths, wildcardPath);
    };
  }

  private static List<String> getPathsInLucene(
      IndexReader indexReader, Optional<FieldPath> embeddedRoot) {
    // getIndexedFields() returns only fields that have terms associated with them (non-numeric).
    return FieldInfos.getIndexedFields(indexReader).stream()
        // Filter fields that are at the correct level of embedding, given an optional embedded root
        // path. If the embedded root path is empty, ensure returned fields are not part of an
        // embedded document.
        .filter(FieldName.EmbeddedField.isAtEmbeddedRoot(embeddedRoot))
        // Filters "$meta", "$multi" and "$facet" fields from lucenePaths as that is not needed.
        .filter(path -> !FieldName.MetaField.isMetaField(path))
        .filter(path -> !FieldName.MultiField.isTypeOf(path))
        .filter(path -> !FieldName.StaticField.FACET.isTypeOf(path))
        .map(FieldName::stripAnyPrefixFromLuceneFieldName)
        .collect(Collectors.toList());
  }

  /**
   * Accepts a wildcard path and compares it with all of the indexed paths in Lucene to return a
   * stream of matching ones.
   */
  private static Stream<StringPath> resolveWildcardPath(
      List<String> processedLucenePaths, UnresolvedStringWildcardPath wildcardPath) {
    Pattern wildcardRegex = wildcardPathToRegex(wildcardPath);

    return processedLucenePaths.stream()
        .filter(wildcardRegex.asMatchPredicate())
        .map(FieldPath::parse)
        .map(StringFieldPath::new);
  }

  /** Convert an unresolved path (e.g. "a.*") to a path regex (e.g. "\Qa.\E.*?") */
  private static Pattern wildcardPathToRegex(UnresolvedStringWildcardPath path) {
    String wildcardString = path.toString();
    String regex =
        STAR_SPLITTER
            .splitToStream(wildcardString)
            .map(Pattern::quote)
            .collect(Collectors.joining(WILDCARD_REGEX));
    return Pattern.compile(regex);
  }
}
