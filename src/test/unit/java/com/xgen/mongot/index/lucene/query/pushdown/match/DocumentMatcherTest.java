package com.xgen.mongot.index.lucene.query.pushdown.match;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.FieldPath;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import org.bson.BsonArray;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.bson.io.BasicOutputBuffer;
import org.junit.Test;

public class DocumentMatcherTest {

  private static final BsonArray simpleArrayValue =
      new BsonArray(List.of(new BsonInt64(0), new BsonInt64(1)));

  private static final RawBsonDocument doc =
      BsonUtils.documentToRaw(
          new BsonDocument()
              .append("$path", new BsonString("EscapedPath"))
              .append("dotted.path", new BsonString("Illegal"))
              .append("top", new BsonDouble(1.0))
              .append("simpleArray", simpleArrayValue)
              .append(
                  "array",
                  new BsonArray(
                      List.of(
                          new BsonDateTime(1),
                          new BsonDocument("foo", new BsonInt64(1)),
                          new BsonDocument("foo", new BsonInt64(2)),
                          new BsonArray(List.of(new BsonDocument("foo", new BsonInt64(3)))),
                          new BsonDocument(
                              "nested", new BsonArray(List.of(new BsonString("foo"))))))));

  private static class CollectingPredicate implements Predicate<BsonValue> {

    final List<BsonValue> tested = new ArrayList<>();

    @Override
    public boolean test(BsonValue bsonValue) {
      this.tested.add(bsonValue);
      return false;
    }
  }

  private static void assertMatch(Predicate<BsonValue> test, String path) {
    assertTrue(DocumentMatcher.matches(doc, test, FieldPath.parse(path)));
  }

  private static void assertNoMatch(Predicate<BsonValue> test, String path) {
    assertFalse(DocumentMatcher.matches(doc, test, FieldPath.parse(path)));
  }

  @Test
  public void repeatedKeyReadsOnlyFirst() {
    BasicOutputBuffer out = new BasicOutputBuffer();
    BsonBinaryWriter writer = new BsonBinaryWriter(out);
    writer.writeStartDocument();
    writer.writeDouble("top", 1.0);
    writer.writeBoolean("x", false);
    writer.writeBoolean("x", true);
    writer.writeEndDocument();
    RawBsonDocument doc = new RawBsonDocument(out.toByteArray());

    boolean hasFalse =
        DocumentMatcher.matches(
            doc, v -> v.isBoolean() && !v.asBoolean().getValue(), FieldPath.newRoot("x"));
    assertTrue(hasFalse);

    boolean hasTrue =
        DocumentMatcher.matches(
            doc, v -> v.isBoolean() && v.asBoolean().getValue(), FieldPath.newRoot("x"));
    assertFalse(hasTrue);
  }

  @Test
  public void matches() {
    assertMatch(v -> v.equals(new BsonDouble(1.0)), "top");
    assertMatch(v -> v.equals(new BsonDateTime(1)), "array");
    assertMatch(BsonValue::isNull, "array.nested");
    assertMatch(v -> v.equals(new BsonString("foo")), "array.nested");
    assertMatch(v -> v.equals(simpleArrayValue), "simpleArray");
    assertMatch(v -> v.equals(new BsonInt64(0)), "simpleArray");
  }

  @Test
  public void noMatch() {
    assertNoMatch(BsonValue::isArray, "top");
    assertNoMatch(BsonValue::isString, "array");
    assertNoMatch(BsonValue::isDateTime, "array.nested");
    assertNoMatch(BsonValue::isDocument, "array.nested");
  }

  @Test
  public void traverseEachDocumentInArray() {
    CollectingPredicate collector = new CollectingPredicate();

    assertNoMatch(collector, "array.foo");

    assertThat(collector.tested)
        .containsExactly(new BsonInt64(1), new BsonInt64(2), BsonNull.VALUE);
  }

  @Test
  public void dottedPathIgnored() {
    CollectingPredicate collector = new CollectingPredicate();

    assertNoMatch(collector, "dotted.path");

    assertThat(collector.tested).containsExactly(BsonNull.VALUE);
  }

  @Test
  public void dollarPath() {
    CollectingPredicate collector = new CollectingPredicate();

    assertNoMatch(collector, "$path");

    assertThat(collector.tested).containsExactly(new BsonString("EscapedPath"));
  }
}
