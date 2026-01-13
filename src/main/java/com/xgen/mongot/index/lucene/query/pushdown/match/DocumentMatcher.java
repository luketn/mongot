package com.xgen.mongot.index.lucene.query.pushdown.match;

import com.google.common.collect.Lists;
import com.google.errorprone.annotations.Var;
import com.xgen.mongot.util.FieldPath;
import java.util.List;
import java.util.function.Predicate;
import org.bson.BsonNull;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;

/**
 * This utility class tests BsonValues against a {@link Predicate} at a given{@link FieldPath} in a
 * BsonDocument. The path semantics used by this class mimic that of the $match stage of the
 * aggregation pipeline. The semantics of $match differs from $search in a couple notable ways:
 *
 * <ol>
 *   <li>Operators are broadcast against one level of arrays: nested arrays are not expanded.
 *   <li>Document keys containing a '.' will never match a predicate.
 *   <li>For repeated keys, only the first value is considered.
 * </ol>
 */
class DocumentMatcher {

  private static List<String> segments(FieldPath path) {
    return Lists.transform(path.getPathHierarchy(), FieldPath::getLeaf);
  }

  /**
   * Returns true if any value in `document` with path `path` matches the given predicate.
   *
   * <p>The path will be interpreted in the same way the $match stage interprets paths.
   */
  public static boolean matches(
      RawBsonDocument document, Predicate<BsonValue> predicate, FieldPath path) {
    return matches(document, segments(path), predicate);
  }

  private static boolean matches(
      RawBsonDocument raw, List<String> segments, Predicate<BsonValue> predicate) {
    @Var BsonValue root = raw;
    for (int i = 0; i < segments.size(); i++) {
      String segment = segments.get(i);
      if (root.isDocument()) {
        root = root.asDocument().get(segment);
        if (root == null) {
          return predicate.test(BsonNull.VALUE);
        }
      } else if (root.isArray()) {
        // Don't use array.get(i)!
        var remainder = segments.subList(i, segments.size());
        for (BsonValue child : root.asArray()) {
          if (elementMatch(child, remainder, predicate)) {
            return true;
          }
        }
        return false;
      } else {
        return predicate.test(BsonNull.VALUE);
      }
    }

    return root.isArray()
        ? predicate.test(root) || root.asArray().stream().anyMatch(predicate)
        : predicate.test(root);
  }

  /**
   * Helper function to broadcast predicate across all array children.
   *
   * @param arrayElement - a child of an BsonArray
   * @param remainingSegments - a list of path segments relative to the current subtree
   * @return true if an element in the subtree with path `path` matches the given predicate.
   */
  private static boolean elementMatch(
      BsonValue arrayElement, List<String> remainingSegments, Predicate<BsonValue> predicate) {
    if (remainingSegments.isEmpty()) {
      return predicate.test(arrayElement);
    } else {
      if (arrayElement.isDocument()) {
        return DocumentMatcher.matches(
            (RawBsonDocument) arrayElement.asDocument(), remainingSegments, predicate);
      } else {
        // Do not expand directly nested arrays.
        return false;
      }
    }
  }
}
