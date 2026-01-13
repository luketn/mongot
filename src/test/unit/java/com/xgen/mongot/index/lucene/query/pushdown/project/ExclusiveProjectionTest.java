package com.xgen.mongot.index.lucene.query.pushdown.project;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.util.BsonDocumentUtil;
import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.RawBsonDocument;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

@RunWith(Theories.class)
public class ExclusiveProjectionTest {

  private static void assertEq(RawBsonDocument expected, RawBsonDocument actual) {
    var expectedJson = BsonDocumentUtil.decodeCorruptDocument(expected);
    var actualJson = BsonDocumentUtil.decodeCorruptDocument(actual);
    assertEquals(expectedJson + "!= " + actualJson, expected, actual);
  }

  @DataPoints
  public static ImmutableList<RawBsonDocument> documents() {
    return Stream.of(
            new BsonDocument("d", new BsonInt64(0)),
            new BsonDocument()
                .append("int", new BsonInt64(1))
                .append("doc", new BsonDocument("foo", new BsonInt64(2))),
            new BsonDocument()
                .append("int", new BsonInt64(1))
                .append("doc", new BsonDocument("foo", new BsonArray(List.of()))),
            new BsonDocument()
                .append("int", new BsonInt64(1))
                .append(
                    "doc",
                    new BsonDocument("foo", new BsonDocument("bar", new BsonString("value")))))
        .map(BsonUtils::documentToRaw)
        .collect(toImmutableList());
  }

  @Theory
  public void noExclusions(RawBsonDocument raw) throws IOException {
    ExclusiveBlobProjection projector =
        new ExclusiveBlobProjection(new ProjectSpec(ImmutableList.of(), ImmutableList.of(), false));

    RawBsonDocument result = projector.project(raw);

    assertEq(raw, result);
  }

  @Theory
  public void excludeMissingFields(RawBsonDocument raw) throws IOException {
    var exclusions =
        Stream.of("missing", "missing.field", "missing.nested.document.field")
            .map(FieldPath::parse)
            .collect(toImmutableList());
    ExclusiveBlobProjection projector =
        new ExclusiveBlobProjection(new ProjectSpec(ImmutableList.of(), exclusions, false));

    RawBsonDocument result = projector.project(raw);
    assertEq(raw, result);
  }

  @Test
  public void excludeScalar() throws IOException {
    RawBsonDocument raw =
        BsonUtils.documentToRaw(
            new BsonDocument()
                .append("int", new BsonInt64(1))
                .append("long", new BsonInt64(1))
                .append(
                    "doc",
                    new BsonDocument("foo", new BsonDocument("bar", new BsonString("value")))));
    var exclusions = ImmutableList.of(FieldPath.parse("int"));
    ExclusiveBlobProjection projector =
        new ExclusiveBlobProjection(new ProjectSpec(ImmutableList.of(), exclusions, false));

    RawBsonDocument result = projector.project(raw);

    RawBsonDocument expected =
        BsonUtils.documentToRaw(
            new BsonDocument("long", new BsonInt64(1))
                .append(
                    "doc",
                    new BsonDocument("foo", new BsonDocument("bar", new BsonString("value")))));
    assertEq(expected, result);
  }

  @Test
  public void excludeDocument() throws IOException {
    RawBsonDocument raw =
        BsonUtils.documentToRaw(
            new BsonDocument()
                .append("int", new BsonInt64(1))
                .append(
                    "doc",
                    new BsonDocument("foo", new BsonDocument("bar", new BsonString("value")))));
    var exclusions = ImmutableList.of(FieldPath.parse("doc"));
    ExclusiveBlobProjection projector =
        new ExclusiveBlobProjection(new ProjectSpec(ImmutableList.of(), exclusions, false));

    RawBsonDocument result = projector.project(raw);

    RawBsonDocument expected = BsonUtils.documentToRaw(new BsonDocument("int", new BsonInt64(1)));
    assertEq(expected, result);
  }

  @Test
  public void excludeMiddleDocument() throws IOException {
    RawBsonDocument raw =
        BsonUtils.documentToRaw(
            new BsonDocument()
                .append("int", new BsonInt64(1))
                .append(
                    "doc",
                    new BsonDocument("foo", new BsonDocument("bar", new BsonString("value")))));
    var exclusions = ImmutableList.of(FieldPath.parse("doc.foo"));
    ExclusiveBlobProjection projector =
        new ExclusiveBlobProjection(new ProjectSpec(ImmutableList.of(), exclusions, false));

    RawBsonDocument result = projector.project(raw);

    RawBsonDocument expected =
        BsonUtils.documentToRaw(
            new BsonDocument().append("int", new BsonInt64(1)).append("doc", new BsonDocument()));
    assertEq(expected, result);
  }

  @Test
  public void excludeLeaf() throws IOException {
    RawBsonDocument raw =
        BsonUtils.documentToRaw(
            new BsonDocument()
                .append("int", new BsonInt64(1))
                .append("long", new BsonInt64(1))
                .append(
                    "doc",
                    new BsonDocument("foo", new BsonDocument("bar", new BsonString("value")))));
    var exclusions = ImmutableList.of(FieldPath.parse("doc.foo.bar"));
    ExclusiveBlobProjection projector =
        new ExclusiveBlobProjection(new ProjectSpec(ImmutableList.of(), exclusions, false));

    RawBsonDocument result = projector.project(raw);

    RawBsonDocument expected =
        BsonUtils.documentToRaw(
            new BsonDocument()
                .append("int", new BsonInt64(1))
                .append("long", new BsonInt64(1))
                .append("doc", new BsonDocument("foo", new BsonDocument())));
    assertEq(expected, result);
  }

  @Test
  public void excludeArrayOfDocuments() throws IOException {
    RawBsonDocument raw = RawBsonDocument.parse("{a: 1, b: [{c:1}, {}, {c: 4}, true]}");
    var exclusions = ImmutableList.of(FieldPath.parse("b.c"));
    ExclusiveBlobProjection projector =
        new ExclusiveBlobProjection(new ProjectSpec(ImmutableList.of(), exclusions, false));

    RawBsonDocument result = projector.project(raw);

    RawBsonDocument expected = RawBsonDocument.parse("{a: 1, b: [{}, {}, {}, true]}");
    assertEq(expected, result);
  }

  @Test
  public void repeatedKeys() throws IOException {
    RawBsonDocument raw = RawBsonDocument.parse("{a: 1, a: null, foo: {b: null, b: 2}}");
    var exclusions = ImmutableList.of(FieldPath.parse("a"), FieldPath.parse("foo.b"));
    ExclusiveBlobProjection projector =
        new ExclusiveBlobProjection(new ProjectSpec(ImmutableList.of(), exclusions, false));

    RawBsonDocument result = projector.project(raw);

    RawBsonDocument expected = RawBsonDocument.parse("{a: null, foo: {b: 2}}");
    assertEq(expected, result);
  }
}
