package com.xgen.mongot.index.ingestion;

import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.index.ingestion.serialization.LuceneIndexedGeoShapeField;
import com.xgen.testing.mongot.index.ingestion.serialization.LuceneIndexedLatLonPointField;
import com.xgen.testing.mongot.index.ingestion.serialization.LuceneIndexedNumericField;
import com.xgen.testing.mongot.index.ingestion.serialization.LuceneIndexedStoredSourceField;
import com.xgen.testing.mongot.index.ingestion.serialization.LuceneIndexedStringField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.Test;

public class LuceneIndexedFieldSpecTest {

  @Test
  public void testEqualityGroups() {
    TestUtils.assertEqualityGroups(
        () -> new LuceneIndexedNumericField(0L, true, false, DocValuesType.NUMERIC),
        () -> new LuceneIndexedNumericField(0L, true, false, DocValuesType.SORTED_SET),
        () -> new LuceneIndexedNumericField(0L, true, false, DocValuesType.SORTED_NUMERIC),
        () -> new LuceneIndexedNumericField(0L, true, false, DocValuesType.NONE),
        () -> new LuceneIndexedNumericField(0L, true, false, DocValuesType.SORTED),
        () -> new LuceneIndexedNumericField(0L, true, false, DocValuesType.BINARY),
        () -> new LuceneIndexedNumericField(1L, true, false, DocValuesType.NUMERIC),
        () -> new LuceneIndexedNumericField(1L, true, false, DocValuesType.SORTED_SET),
        () -> new LuceneIndexedNumericField(1L, true, false, DocValuesType.SORTED_NUMERIC),
        () -> new LuceneIndexedNumericField(1L, true, false, DocValuesType.NONE),
        () -> new LuceneIndexedNumericField(1L, true, false, DocValuesType.SORTED),
        () -> new LuceneIndexedNumericField(1L, true, false, DocValuesType.BINARY),
        () -> new LuceneIndexedNumericField(0L, false, false, DocValuesType.NUMERIC),
        () -> new LuceneIndexedNumericField(0L, false, true, DocValuesType.NUMERIC),
        () ->
            new LuceneIndexedStringField(
                "a", false, false, false, DocValuesType.NONE, IndexOptions.DOCS),
        () ->
            new LuceneIndexedStringField(
                "b", false, false, false, DocValuesType.NONE, IndexOptions.DOCS),
        () ->
            new LuceneIndexedStringField(
                "a", true, false, false, DocValuesType.NONE, IndexOptions.DOCS),
        () ->
            new LuceneIndexedStringField(
                "a", false, true, false, DocValuesType.NONE, IndexOptions.DOCS),
        () ->
            new LuceneIndexedStringField(
                "a", false, false, true, DocValuesType.NONE, IndexOptions.DOCS),
        () ->
            new LuceneIndexedStringField(
                "a", false, false, false, DocValuesType.SORTED, IndexOptions.DOCS),
        () ->
            new LuceneIndexedStringField(
                "a", false, false, false, DocValuesType.NONE, IndexOptions.DOCS_AND_FREQS),
        () ->
            new LuceneIndexedGeoShapeField(
                "119304647, 238609294 119304647, 238609294 119304647, 238609294 [true,true,true]"),
        () ->
            new LuceneIndexedGeoShapeField(
                "219304647, 238609294 119304647, 238609294 119304647, 238609294 [true,true,true]"),
        () ->
            new LuceneIndexedLatLonPointField(
                "LatLonPoint <$type:geoPoint/geo:9.999999990686774,9.999999990686774>"),
        () ->
            new LuceneIndexedLatLonPointField(
                "LatLonPoint <$type:geoPoint/geo:9.999999990686774,9.999999990686775>"),
        () -> new LuceneIndexedStoredSourceField(new BsonDocument()),
        () ->
            new LuceneIndexedStoredSourceField(
                new BsonDocument().append("foo", new BsonString("bar"))),
        () ->
            new LuceneIndexedStoredSourceField(
                new BsonDocument().append("foo", new BsonString("baz"))));
  }
}
