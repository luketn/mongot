package com.xgen.mongot.index.ingestion.pushdown;

import static com.xgen.testing.LuceneTestUtils.assertFieldsEquals;
import static com.xgen.testing.LuceneTestUtils.document;

import com.google.common.primitives.Bytes;
import com.xgen.mongot.index.lucene.util.LuceneDoubleConversionUtils;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.LuceneIndexRule;
import java.util.Arrays;
import java.util.List;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.KeywordField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.util.BytesRef;
import org.bson.BsonBinary;
import org.bson.BsonDbPointer;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonJavaScript;
import org.bson.BsonJavaScriptWithScope;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.BsonSymbol;
import org.bson.BsonUndefined;
import org.bson.types.ObjectId;
import org.junit.ClassRule;
import org.junit.Test;

public class MqlFieldFactoryTest {

  @ClassRule public static final LuceneIndexRule validator = new LuceneIndexRule();

  @Test
  public void doubles() {
    Document d = new Document();

    MqlFieldFactory.addField(FieldPath.parse("foo.bar"), new BsonDouble(-0.0), d);
    MqlFieldFactory.addField(FieldPath.parse("foo.bar"), new BsonDouble(Double.NaN), d);
    MqlFieldFactory.addField(FieldPath.parse("foo.baz"), new BsonDouble(0.0), d);

    Document expected =
        document(
            new LongField(
                "$mqlBson:double/foo.bar",
                LuceneDoubleConversionUtils.NAN_SENTINEL,
                Field.Store.NO),
            new LongPoint("$mqlBson:double/foo.bar", 0),
            new SortedNumericDocValuesField("$mqlBson:double/foo.bar", -1L),
            new LongField("$mqlBson:double/foo.baz", 0, Field.Store.NO));
    assertFieldsEquals(expected, d);
    validator.add(d);
  }

  @Test
  public void tooLarge() {
    Document d = new Document();

    MqlFieldFactory.addField(FieldPath.parse("string"), new BsonString("a".repeat(50_000)), d);
    MqlFieldFactory.addField(FieldPath.parse("symbol"), new BsonSymbol("a".repeat(50_000)), d);
    MqlFieldFactory.addField(FieldPath.parse("js"), new BsonJavaScript("a".repeat(50_000)), d);
    MqlFieldFactory.addField(
        FieldPath.parse("scope"),
        new BsonJavaScriptWithScope("a".repeat(50_000), new BsonDocument()),
        d);
    MqlFieldFactory.addField(FieldPath.parse("binary"), new BsonBinary(new byte[50_000]), d);

    Document expected =
        document(
            new NumericDocValuesField(
                "$mqlBson:fallback_marker/string", FallbackMarker.VALUE_TOO_LARGE.id),
            new NumericDocValuesField(
                "$mqlBson:fallback_marker/symbol", FallbackMarker.VALUE_TOO_LARGE.id),
            new NumericDocValuesField(
                "$mqlBson:fallback_marker/js", FallbackMarker.VALUE_TOO_LARGE.id),
            new NumericDocValuesField("$mqlBson:fallback_marker/scope", FallbackMarker.OBJECT.id),
            new NumericDocValuesField(
                "$mqlBson:fallback_marker/binary", FallbackMarker.VALUE_TOO_LARGE.id));
    assertFieldsEquals(expected, d);
    validator.add(d);
  }

  @Test
  public void fallbackMarker() {
    Document d = new Document();

    MqlFieldFactory.addObjectMarker(FieldPath.newRoot("test"), d);

    Document expected =
        document(
            new NumericDocValuesField("$mqlBson:fallback_marker/test", FallbackMarker.OBJECT.id));
    assertFieldsEquals(expected, d);
    validator.add(d);
  }

  @Test
  public void mixed() {
    Document d = new Document();

    MqlFieldFactory.addField(FieldPath.parse("foo.bar"), new BsonString("string"), d);
    MqlFieldFactory.addField(FieldPath.parse("foo.bar"), new BsonSymbol("symbol"), d);
    MqlFieldFactory.addField(FieldPath.parse("foo.bar"), BsonNull.VALUE, d);
    MqlFieldFactory.addField(FieldPath.parse("foo.bar"), BsonUtils.MAX_KEY, d);
    MqlFieldFactory.addField(FieldPath.parse("foo.bar"), BsonUtils.MIN_KEY, d);
    MqlFieldFactory.addField(FieldPath.parse("foo.bar"), new BsonUndefined(), d);

    Document expected =
        document(
            new KeywordField("$mqlBson:string/foo.bar", new BytesRef("string"), Store.NO),
            new KeywordField("$mqlBson:symbol/foo.bar", new BytesRef("symbol"), Store.NO),
            new KeywordField("$mqlBson:null/foo.bar", new BytesRef("1"), Store.NO),
            new KeywordField("$mqlBson:min_key/foo.bar", new BytesRef("1"), Store.NO),
            new KeywordField("$mqlBson:max_key/foo.bar", new BytesRef("1"), Store.NO),
            new KeywordField("$mqlBson:undefined/foo.bar", new BytesRef("1"), Store.NO));
    assertFieldsEquals(expected, d);
    validator.add(d);
  }

  @Test
  public void dbref() {
    Document d = new Document();
    byte[] oid = new byte[12];
    Arrays.fill(oid, (byte) 0xFF);
    MqlFieldFactory.addField(
        FieldPath.parse("dbref"), new BsonDbPointer("😀unicodé", new ObjectId(oid)), d);

    List<Integer> expectedInts =
        List.of(
            0, 0, 0, 0x0c, 0xf0, 0x9f, 0x98, 0x80, 0x75, 0x6e, 0x69, 0x63, 0x6f, 0x64, 0xc3, 0xa9,
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff);
    byte[] expectedBytes = Bytes.toArray(expectedInts);
    Document expected =
        document(new KeywordField("$mqlBson:db_ref/dbref", new BytesRef(expectedBytes), Store.NO));
    assertFieldsEquals(expected, d);
    validator.add(d);
  }
}
