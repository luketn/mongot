package com.xgen.mongot.index.lucene.explain.information.creator;

import com.xgen.mongot.index.lucene.explain.information.TermRangeQuerySpec;
import com.xgen.mongot.index.lucene.field.FieldName;
import java.util.Arrays;
import java.util.Optional;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.util.BytesRef;
import org.bson.types.ObjectId;

public class TermRangeQuerySpecCreator {
  static TermRangeQuerySpec fromQuery(TermRangeQuery q) {
    return new TermRangeQuerySpec(
        LuceneQuerySpecificationCreator.strip(q.getField()), formatRange(q));
  }

  /**
   * Formats a TermRangeQuery using interval notation. e.g. TermRangeQuery(5, 10, true, false) =>
   * [5, 10)
   */
  private static String formatRange(TermRangeQuery q) {
    String left = q.includesLower() ? "[" : "(";
    String right = q.includesUpper() ? "]" : ")";
    String lower =
        Optional.ofNullable(q.getLowerTerm())
            .map(bytesRef -> getString(bytesRef, q.getField()))
            .orElse("*");
    String upper =
        Optional.ofNullable(q.getUpperTerm())
            .map(bytesRef -> getString(bytesRef, q.getField()))
            .orElse("*");

    return left + lower + ", " + upper + right;
  }

  private static String getString(BytesRef bytesRef, String luceneField) {
    var bytes = BytesRef.deepCopyOf(bytesRef).bytes;
    try {
      // The BytesRef value may represent an ObjectId, so check if it is one
      if (FieldName.TypeField.OBJECT_ID.isTypeOf(luceneField) && bytes.length == 12) {
        return new ObjectId(bytes).toHexString();
      }
      return bytesRef.utf8ToString();
    } catch (RuntimeException e) {
      // in case fields that index binary-typed data byte array values can't convert the bytes to
      // UTF-16
      return Arrays.toString(bytes);
    }
  }
}
