package com.xgen.mongot.index.lucene.query.pushdown;

import com.xgen.mongot.index.lucene.query.sort.mixed.SortUtil;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Comparator;
import org.bson.BsonDbPointer;
import org.bson.BsonDocument;
import org.bson.BsonJavaScriptWithScope;
import org.bson.BsonNumber;
import org.bson.BsonRegularExpression;
import org.bson.BsonType;
import org.bson.BsonUndefined;
import org.bson.BsonValue;

public class MqlComparator {

  public static final Comparator<BsonValue> MIN_SELECTING_COMPARATOR =
      (x, y) -> compareValues(x, y, ArrayComparator.MIN);
  public static final Comparator<BsonValue> MAX_SELECTING_COMPARATOR =
      (x, y) -> compareValues(x, y, ArrayComparator.MAX);
  public static final Comparator<BsonValue> LEXICOGRAPHIC_COMPARATOR =
      (x, y) -> compareValues(x, y, ArrayComparator.LEXICOGRAPHIC);

  public static final BsonUndefined UNDEFINED = new BsonUndefined();

  public static int compareValues(BsonValue first, BsonValue second, ArrayComparator selector) {
    // Check if values fall into different type brackets
    BsonType firstType = first.getBsonType();
    BsonType secondType = second.getBsonType();
    if (firstType == secondType) {
      return compareWithinBracketUnsafe(first, second, selector);
    }

    int typeCompare = compareBracket(firstType, secondType);
    if (typeCompare != 0) {
      return typeCompare;
    }

    // From here on, everything has same bracket but possibly different types
    return compareWithinBracketUnsafe(first, second, selector);
  }

  public static int compareBracket(BsonType first, BsonType second) {
    return Integer.compare(getBracketPriority(first), getBracketPriority(second));
  }

  /**
   * Return the bracket for the given BsonType.
   *
   * <p>Bracket numbers are ordered such that the MQL sort ordering of two Bson types is equivalent
   * to ordering by their priority.
   */
  public static int getBracketPriority(BsonType type) {
    // See reference:
    // https://github.com/mongodb/mongo/blob/7c272f4ca093eb11dccdfe066802a88d037257ff/src/mongo/db/storage/key_string.cpp#L89
    return switch (type) {
      case MIN_KEY -> 0;
      case UNDEFINED ->
          // Note: UNDEFINED == NULL, but sorts strictly before null.
          1;
      case END_OF_DOCUMENT, NULL -> 2;
      case DOUBLE, INT32, INT64, DECIMAL128 -> 3;
      case SYMBOL, STRING -> 4;
      case DOCUMENT -> 5;
      case ARRAY -> 6;
      case BINARY -> 7;
      case OBJECT_ID -> 8;
      case BOOLEAN -> 9;
      case DATE_TIME -> 10;
      case TIMESTAMP -> 11;
      case REGULAR_EXPRESSION -> 12;
      case DB_POINTER -> 13;
      case JAVASCRIPT -> 14;
      case JAVASCRIPT_WITH_SCOPE -> 15;
      case MAX_KEY -> 127;
    };
  }

  public static int compare(
      BsonJavaScriptWithScope left, BsonJavaScriptWithScope right, ArrayComparator selector) {
    int codeComp = left.getCode().compareTo(right.getCode());
    if (codeComp != 0) {
      return codeComp;
    }

    return compare(left.getScope(), right.getScope(), selector);
  }

  @SuppressWarnings("checkstyle:MissingJavadocMethod")
  public static int compare(BsonDocument left, BsonDocument right, ArrayComparator selector) {
    // See https://www.mongodb.com/docs/manual/reference/bson-type-comparison-order/#objects
    var l = left.entrySet().iterator();
    var r = right.entrySet().iterator();

    while (l.hasNext() && r.hasNext()) {
      var leftEntry = l.next();
      var rightEntry = r.next();
      int typeCmp =
          compareBracket(leftEntry.getValue().getBsonType(), rightEntry.getValue().getBsonType());
      if (typeCmp != 0) {
        return typeCmp;
      }

      int nameCmp = leftEntry.getKey().compareTo(rightEntry.getKey());
      if (nameCmp != 0) {
        return nameCmp;
      }

      int valueCmp =
          compareWithinBracketUnsafe(leftEntry.getValue(), rightEntry.getValue(), selector);
      if (valueCmp != 0) {
        return valueCmp;
      }
    }

    return Boolean.compare(l.hasNext(), r.hasNext());
  }

  private static int compare(BsonRegularExpression left, BsonRegularExpression right) {
    int cmp = left.getPattern().compareTo(right.getPattern());
    if (cmp != 0) {
      return cmp;
    }
    return left.getOptions().compareTo(right.getOptions());
  }

  private static int compare(BsonDbPointer left, BsonDbPointer right) {
    byte[] lbytes = left.getNamespace().getBytes(StandardCharsets.UTF_8);
    byte[] rbytes = right.getNamespace().getBytes(StandardCharsets.UTF_8);

    int lengthCmp = Integer.compare(lbytes.length, rbytes.length);
    if (lengthCmp != 0) {
      return lengthCmp;
    }

    int namespaceCmp = Arrays.compareUnsigned(lbytes, rbytes);
    if (namespaceCmp != 0) {
      return namespaceCmp;
    }

    return left.getId().compareTo(right.getId());
  }

  public static int compare(BsonNumber left, BsonNumber right) {
    return switch (left.getBsonType()) {
      case INT64 ->
          MqlNumberUtils.compare(
              left.longValue(), right); // int can promote to double without loss of precision.
      case INT32, DOUBLE -> MqlNumberUtils.compare(left.doubleValue(), right);
      case DECIMAL128 -> compare(left.asDecimal128(), right);
      default -> throw new AssertionError();
    };
  }

  /**
   * Compare two BsonValues which may be different types but are known to be within the same bracket
   *
   * <p>For example, this method can compare BsonInt64 to BsonDouble but not to BsonString.
   */
  public static int compareWithinBracketUnsafe(
      BsonValue left, BsonValue right, ArrayComparator selector) {
    return switch (left.getBsonType()) {
      case NULL, UNDEFINED, MIN_KEY, MAX_KEY, END_OF_DOCUMENT ->
          // If values have same bracket and one is a singleton type, they must be equal
          0;

      // right.asNumber() fails for Decimal128, we need an explicit cast.
      case INT32 -> MqlNumberUtils.compare(left.asInt32().intValue(), (BsonNumber) right);
      case INT64 -> MqlNumberUtils.compare(left.asInt64().longValue(), (BsonNumber) right);
      case DOUBLE -> MqlNumberUtils.compare(left.asDouble().doubleValue(), (BsonNumber) right);
      case DECIMAL128 ->
          MqlNumberUtils.compare(
              left.asDecimal128().decimal128Value(), ((BsonNumber) right).decimal128Value());
      case STRING ->
          left.asString()
              .getValue()
              .compareTo(
                  right.isString() ? right.asString().getValue() : right.asSymbol().getSymbol());
      case SYMBOL ->
          left.asSymbol()
              .getSymbol()
              .compareTo(
                  right.isString() ? right.asString().getValue() : right.asSymbol().getSymbol());
      case DATE_TIME -> Long.compare(left.asDateTime().getValue(), right.asDateTime().getValue());
      case BINARY -> SortUtil.compare(left.asBinary(), right.asBinary());
      case OBJECT_ID -> left.asObjectId().compareTo(right.asObjectId());
      case BOOLEAN -> left.asBoolean().compareTo(right.asBoolean());
      case TIMESTAMP -> left.asTimestamp().compareTo(right.asTimestamp());
      case JAVASCRIPT -> left.asJavaScript().getCode().compareTo(right.asJavaScript().getCode());
      case JAVASCRIPT_WITH_SCOPE ->
          compare(left.asJavaScriptWithScope(), right.asJavaScriptWithScope(), selector);
      case DB_POINTER -> compare(left.asDBPointer(), right.asDBPointer());
      case REGULAR_EXPRESSION -> compare(left.asRegularExpression(), right.asRegularExpression());
      case DOCUMENT -> compare(left.asDocument(), right.asDocument(), selector);
      case ARRAY -> selector.compare(left.asArray(), right.asArray());
    };
  }
}
