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
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.RawBsonDocument;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

@RunWith(Theories.class)
public class InclusiveBlobProjectTest {

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
  public void includeMissingFields(RawBsonDocument raw) throws IOException {
    var fields =
        Stream.of("missing", "missing.field", "missing.nested.document.field")
            .map(FieldPath::parse)
            .collect(toImmutableList());
    InclusiveBlobProject projector =
        new InclusiveBlobProject(new ProjectSpec(fields, ImmutableList.of(), false));

    RawBsonDocument result = projector.project(raw);

    assertEq(RawBsonDocument.parse("{}"), result);
  }

  @Test
  public void includeScalar() throws IOException {
    RawBsonDocument raw =
        BsonUtils.documentToRaw(
            new BsonDocument()
                .append("int", new BsonInt32(1))
                .append("long", new BsonInt64(2))
                .append(
                    "doc",
                    new BsonDocument("foo", new BsonDocument("bar", new BsonString("value")))));
    var fields = ImmutableList.of(FieldPath.parse("int"));
    InclusiveBlobProject projector =
        new InclusiveBlobProject(new ProjectSpec(fields, ImmutableList.of(), false));

    RawBsonDocument result = projector.project(raw);

    RawBsonDocument expected = BsonUtils.documentToRaw(new BsonDocument("int", new BsonInt32(1)));
    assertEq(expected, result);
  }

  @Test
  public void includeFullDocument() throws IOException {
    RawBsonDocument raw = RawBsonDocument.parse("{int: 1, doc: {foo: {bar: 5, baz: 6}}}");
    var fields = ImmutableList.of(FieldPath.parse("doc"));
    InclusiveBlobProject projector =
        new InclusiveBlobProject(new ProjectSpec(fields, ImmutableList.of(), false));

    RawBsonDocument result = projector.project(raw);

    RawBsonDocument expected = RawBsonDocument.parse("{doc: {foo: {bar: 5, baz: 6}}}");
    assertEq(expected, result);
  }

  @Test
  public void includeMiddleDocument() throws IOException {
    RawBsonDocument raw = RawBsonDocument.parse("{int: 1, doc: {foo: {bar: 5, baz: 6}}}");
    var fields = ImmutableList.of(FieldPath.parse("doc.foo"));
    InclusiveBlobProject projector =
        new InclusiveBlobProject(new ProjectSpec(fields, ImmutableList.of(), false));

    RawBsonDocument result = projector.project(raw);

    RawBsonDocument expected = RawBsonDocument.parse("{doc: {foo: {bar: 5, baz: 6}}}");
    assertEq(expected, result);
  }

  @Test
  public void includeLeafDoc() throws IOException {
    RawBsonDocument raw = RawBsonDocument.parse("{int: 1, doc: {foo: {bar: 5, baz: 6}}}");
    var fields = ImmutableList.of(FieldPath.parse("doc.foo.bar"));
    InclusiveBlobProject projector =
        new InclusiveBlobProject(new ProjectSpec(fields, ImmutableList.of(), false));

    RawBsonDocument result = projector.project(raw);

    RawBsonDocument expected = RawBsonDocument.parse("{doc: {foo: {bar: 5}}}");
    assertEq(expected, result);
  }

  @Test
  public void missingSubdocument() throws IOException {
    RawBsonDocument raw = RawBsonDocument.parse("{int: 1, doc: {foo: {bar: 5, baz: 6}}}");
    var fields = ImmutableList.of(FieldPath.parse("doc.missing"));
    InclusiveBlobProject projector =
        new InclusiveBlobProject(new ProjectSpec(fields, ImmutableList.of(), false));

    RawBsonDocument result = projector.project(raw);

    RawBsonDocument expected = RawBsonDocument.parse("{doc: {}}");
    assertEq(expected, result);
  }

  @Test
  public void missingArraySubpath() throws IOException {
    RawBsonDocument raw = RawBsonDocument.parse("{int: 1, array: [1, 2, 3]}");
    var fields = ImmutableList.of(FieldPath.parse("array.missing"));
    InclusiveBlobProject projector =
        new InclusiveBlobProject(new ProjectSpec(fields, ImmutableList.of(), false));

    RawBsonDocument result = projector.project(raw);

    RawBsonDocument expected = RawBsonDocument.parse("{array: []}");
    assertEq(expected, result);
  }

  @Test
  public void repeatedKeys() throws IOException {
    RawBsonDocument raw =
        RawBsonDocument.parse(
            "{a: 1, a: 2, foo: {b: null, b: 2}, array: [{c: 3}, {c: 4, c:5, d:0}, {d: 0, d:1}]}");
    var fields =
        ImmutableList.of(
            FieldPath.parse("a"), FieldPath.parse("foo.b"), FieldPath.parse("array.c"));
    InclusiveBlobProject projector =
        new InclusiveBlobProject(new ProjectSpec(fields, ImmutableList.of(), false));

    RawBsonDocument result = projector.project(raw);

    RawBsonDocument expected =
        RawBsonDocument.parse("{a: 1, foo: {b: null}, array: [{c: 3}, {c: 4}, {}]}");
    assertEq(expected, result);
  }
}
