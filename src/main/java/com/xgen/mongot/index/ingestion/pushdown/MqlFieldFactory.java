package com.xgen.mongot.index.ingestion.pushdown;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.xgen.mongot.index.lucene.util.LuceneDoubleConversionUtils;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.ByteUtils;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.KeywordField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.bson.BsonBinary;
import org.bson.BsonDateTime;
import org.bson.BsonDbPointer;
import org.bson.BsonRegularExpression;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;

/**
 * This class provides utility methods for indexing documents that preserve MQL aggregation pipeline
 * semantics for $match, $sort, and $project. Values are added to the inverted index to allow for
 * efficient $match pushdown. DocValues provide efficient $sort pushdown and preserve type
 * information for $project.
 */
final class MqlFieldFactory {

  private static final String BOOLEAN_TRUE = "T";

  private static final String BOOLEAN_FALSE = "F";

  private static final BytesRef SINGLETON_VALUE = new BytesRef("1");

  private static final long POSITIVE_ZERO_ENCODING =
      LuceneDoubleConversionUtils.toMqlSortableLong(0.0);

  private static final long NEGATIVE_ZERO_ENCODING =
      LuceneDoubleConversionUtils.toMqlSortableLong(-0.0);

  private MqlFieldFactory() {
    // Util Class
  }

  /**
   * Adds a field for a type that only takes on one value (null, undef, min, max). In this case, the
   * presence or absence of the path name is sufficient to answer any query.
   */
  private static boolean addSingleton(FieldPath path, MqlField type, Document d) {
    String name = type.getFieldName(path);
    d.add(new KeywordField(name, SINGLETON_VALUE, Store.NO));
    return true;
  }

  private static boolean addDouble(FieldPath path, double value, Document d) {
    String name = MqlField.DOUBLE.getFieldName(path);
    long encoded = LuceneDoubleConversionUtils.toMqlSortableLong(value);
    if (encoded == NEGATIVE_ZERO_ENCODING) {
      // Coalesce +/- 0.0 for the index, but preserve the sign for $project
      d.add(new LongPoint(name, POSITIVE_ZERO_ENCODING));
      d.add(new SortedNumericDocValuesField(name, NEGATIVE_ZERO_ENCODING));
    } else {
      d.add(new LongField(name, encoded, Store.NO));
    }
    return true;
  }

  private static boolean addBinary(FieldPath path, BsonBinary value, Document d) {
    byte[] data = value.getData();
    ByteBuffer buf = ByteBuffer.allocate(4 + 1 + data.length);
    buf.putInt(data.length).put(value.getType()).put(data);

    return addBytes(path, MqlField.BINARY, buf.array(), d);
  }

  private static boolean addBoolean(FieldPath path, boolean value, Document d) {
    var ref = new BytesRef(value ? BOOLEAN_TRUE : BOOLEAN_FALSE);
    String name = MqlField.BOOLEAN.getFieldName(path);
    d.add(new KeywordField(name, ref, Store.NO));
    return true;
  }

  private static boolean addInt32(FieldPath path, int value, Document d) {
    // Coalesce ints and longs in index, but preserve type in separate docvalue fields
    d.add(new LongPoint(MqlField.INT64.getFieldName(path), value));
    d.add(new SortedNumericDocValuesField(MqlField.INT32.getFieldName(path), value));
    return true;
  }

  private static boolean addInt64(FieldPath path, long value, Document d) {
    String name = MqlField.INT64.getFieldName(path);
    d.add(new LongField(name, value, Store.NO));
    return true;
  }

  private static boolean addDateTime(FieldPath path, BsonDateTime dateTime, Document d) {
    long value = dateTime.getValue();
    String name = MqlField.DATE_TIME.getFieldName(path);
    d.add(new LongField(name, value, Store.NO));
    return true;
  }

  private static boolean addRegex(FieldPath path, BsonRegularExpression regex, Document d) {
    // https://github.com/mongodb/mongo/blob/ac33e2199055f80f45de4463005781652221c875/src/mongo/db/storage/key_string.cpp#L838
    String value = regex.getOptions() + '\0' + regex.getPattern();
    return addUtf8Text(path, MqlField.REGULAR_EXP, value, d);
  }

  private static boolean addUtf8Text(FieldPath path, MqlField type, String value, Document d) {
    String name = type.getFieldName(path);
    BytesRef utf8 = new BytesRef(value);
    if (utf8.length < IndexWriter.MAX_TERM_LENGTH) {
      d.add(new KeywordField(name, utf8, Store.NO));
      return true;
    } else {
      addFallbackMarker(path, d, FallbackMarker.VALUE_TOO_LARGE);
      return false;
    }
  }

  private static boolean addTimestamp(FieldPath path, BsonTimestamp timestamp, Document d) {
    long value = timestamp.getValue();
    String name = MqlField.TIMESTAMP.getFieldName(path);
    d.add(new LongField(name, value, Store.NO));
    return true;
  }

  private static boolean addDbRef(FieldPath path, BsonDbPointer dbPointer, Document d) {
    // https://github.com/mongodb/mongo/blob/ac33e2199055f80f45de4463005781652221c875/src/mongo/db/storage/key_string.cpp#L848
    byte[] nameBytes = dbPointer.getNamespace().getBytes(StandardCharsets.UTF_8);
    byte[] oid = dbPointer.getId().toByteArray();
    ByteBuffer buffer = ByteBuffer.allocate(4 + nameBytes.length + oid.length);
    byte[] result = buffer.putInt(nameBytes.length).put(nameBytes).put(oid).array();
    BytesRef ref = new BytesRef(result);

    if (ref.length < IndexWriter.MAX_TERM_LENGTH) {
      String name = MqlField.DB_REF.getFieldName(path);
      d.add(new KeywordField(name, ref, Store.NO));
      return true;
    } else {
      addFallbackMarker(path, d, FallbackMarker.VALUE_TOO_LARGE);
      return false;
    }
  }

  /** Index raw byes (no UTF-encoding) under a type-specific path. */
  private static boolean addBytes(FieldPath path, MqlField type, byte[] bytes, Document d) {
    if (bytes.length < IndexWriter.MAX_TERM_LENGTH) {
      String name = type.getFieldName(path);
      var ref = new BytesRef(bytes);
      d.add(new KeywordField(name, ref, Store.NO));
      return true;
    } else {
      addFallbackMarker(path, d, FallbackMarker.VALUE_TOO_LARGE);
      return false;
    }
  }

  @CanIgnoreReturnValue
  static boolean addProjection(FieldPath path, RawBsonDocument value, Document d) {
    BytesRef bytes = ByteUtils.toBytesRef(value);
    String name = MqlField.PROJECTION.getFieldName(path);
    BinaryDocValuesField dv = new BinaryDocValuesField(name, bytes);
    d.add(dv);
    return true;
  }

  public static void addEmptyArray(FieldPath path, Document d) {
    addFallbackMarker(path, d, FallbackMarker.EMPTY_ARRAY);
  }

  /**
   * Notes the presence of an Object at this path or other data that we cannot index efficiently. In
   * this case, queries referencing this path need to take the fallback path.
   */
  static boolean addObjectMarker(FieldPath path, Document d) {
    addFallbackMarker(path, d, FallbackMarker.OBJECT);
    return true;
  }

  private static void addFallbackMarker(FieldPath path, Document d, FallbackMarker type) {
    String name = MqlField.FALLBACK_MARKER.getFieldName(path);
    d.add(new NumericDocValuesField(name, type.id));
  }

  /** Add field(s) to index the given {@link BsonValue}.
   *
   * @param path - the path to value v in the indexed BsonDocument
   * @param v - the value to index
   * @param d - the Lucene {@link Document} to add {@link IndexableField}(s) to.
   *
   * @return the input parameter `d` for chaining fluent calls.
   */
  @CanIgnoreReturnValue
  static boolean addField(FieldPath path, BsonValue v, Document d) {
    return switch (v.getBsonType()) {
      case END_OF_DOCUMENT ->
          // Not a real field type
          Check.unreachable("END_OF_DOCUMENT is not a real field type");
      case DOUBLE -> addDouble(path, v.asDouble().doubleValue(), d);
      case STRING -> addUtf8Text(path, MqlField.STRING, v.asString().getValue(), d);
      case SYMBOL -> addUtf8Text(path, MqlField.SYMBOL, v.asSymbol().getSymbol(), d);
      case DOCUMENT, ARRAY -> addObjectMarker(path, d);
      case BINARY -> addBinary(path, v.asBinary(), d);
      case UNDEFINED -> addSingleton(path, MqlField.UNDEFINED, d);
      case OBJECT_ID ->
          addBytes(path, MqlField.OBJECT_ID, v.asObjectId().getValue().toByteArray(), d);
      case BOOLEAN -> addBoolean(path, v.asBoolean().getValue(), d);
      case DATE_TIME -> addDateTime(path, v.asDateTime(), d);
      case NULL -> addSingleton(path, MqlField.NULL, d);
      case REGULAR_EXPRESSION -> addRegex(path, v.asRegularExpression(), d);
      case DB_POINTER -> addDbRef(path, v.asDBPointer(), d);
      case JAVASCRIPT -> addUtf8Text(path, MqlField.JAVASCRIPT, v.asJavaScript().getCode(), d);
      case JAVASCRIPT_WITH_SCOPE ->
          // Scope is an object and we do not index objects
          addObjectMarker(path, d);
      case INT32 -> addInt32(path, v.asInt32().getValue(), d);
      case TIMESTAMP -> addTimestamp(path, v.asTimestamp(), d);
      case INT64 -> addInt64(path, v.asInt64().getValue(), d);
      case DECIMAL128 ->
          // Use fallback path until we can port keystring encoding
          addObjectMarker(path, d);
      case MIN_KEY -> addSingleton(path, MqlField.MIN_KEY, d);
      case MAX_KEY -> addSingleton(path, MqlField.MAX_KEY, d);
    };
  }
}
