package com.xgen.mongot.index.lucene.query.sort.mixed;

import static com.google.common.truth.Truth.assertThat;

import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.field.FieldValue;
import com.xgen.mongot.index.lucene.util.LuceneDoubleConversionUtils;
import com.xgen.mongot.index.query.sort.MongotSortField;
import com.xgen.mongot.index.query.sort.SortOptions;
import com.xgen.mongot.index.query.sort.UserFieldSortOptions;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.util.RandomSegmentingIndexWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDouble;
import org.bson.BsonInt64;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MqlMixedSortTest {

  private Directory directory;
  private IndexSearcher searcher;

  private static final String stringField =
      FieldName.TypeField.TOKEN.getLuceneFieldName(FieldPath.newRoot("f"), Optional.empty());

  private static final String dateField =
      FieldName.TypeField.DATE_V2.getLuceneFieldName(FieldPath.newRoot("f"), Optional.empty());
  private static final String intField =
      FieldName.TypeField.NUMBER_INT64_V2.getLuceneFieldName(
          FieldPath.newRoot("f"), Optional.empty());
  private static final String doubleField =
      FieldName.TypeField.NUMBER_DOUBLE_V2.getLuceneFieldName(
          FieldPath.newRoot("f"), Optional.empty());
  private static final String uuidField =
      FieldName.TypeField.UUID.getLuceneFieldName(FieldPath.newRoot("f"), Optional.empty());
  private static final String nullField =
      FieldName.TypeField.NULL.getLuceneFieldName(FieldPath.newRoot("f"), Optional.empty());
  private static final String objectIdField =
      FieldName.TypeField.OBJECT_ID.getLuceneFieldName(FieldPath.newRoot("f"), Optional.empty());
  private static final String booleanField =
      FieldName.TypeField.BOOLEAN.getLuceneFieldName(FieldPath.newRoot("f"), Optional.empty());

  /** Sets up test. */
  @Before
  public void setup() throws IOException {
    this.directory = new ByteBuffersDirectory();

    List<Document> docs = new ArrayList<>();
    Document doc0 = new Document();
    doc0.add(new SortedSetDocValuesField(stringField, new BytesRef("1")));
    docs.add(doc0);

    Document doc1 = new Document();
    doc1.add(
        new SortedNumericDocValuesField(
            doubleField, LuceneDoubleConversionUtils.toMqlSortableLong(0.0)));
    docs.add(doc1);

    Document doc2 = new Document();
    doc2.add(new SortedNumericDocValuesField(dateField, 123456789));
    docs.add(doc2);

    Document doc3 = new Document();
    doc3.add(new SortedNumericDocValuesField(intField, 1));
    docs.add(doc3);

    Document doc4 = new Document();
    doc4.add(new SortedSetDocValuesField(stringField, new BytesRef("2")));
    docs.add(doc4);

    Document doc5 = new Document();
    doc5.add(
        new SortedNumericDocValuesField(
            doubleField, LuceneDoubleConversionUtils.toMqlSortableLong(2.0)));
    docs.add(doc5);

    Document doc6 = new Document();
    doc6.add(new SortedNumericDocValuesField(dateField, 923456789));
    docs.add(doc6);

    Document doc7 = new Document();
    doc7.add(new SortedNumericDocValuesField(intField, 3));
    docs.add(doc7);

    Document doc8 = new Document();
    doc8.add(new SortedNumericDocValuesField(dateField, Integer.MAX_VALUE));
    docs.add(doc8);

    Document doc9 = new Document();
    docs.add(doc9);

    Document doc10 = new Document();
    doc10.add(
        new SortedNumericDocValuesField(
            doubleField, LuceneDoubleConversionUtils.toMqlSortableLong(Double.NaN)));
    docs.add(doc10);

    Document doc11 = new Document();
    doc11.add(
        new SortedSetDocValuesField(
            uuidField, new BytesRef("11111111-1111-1111-1111-111111111111")));
    docs.add(doc11);

    Document doc12 = new Document();
    doc12.add(
        new SortedSetDocValuesField(
            uuidField, new BytesRef("22222222-2222-2222-2222-222222222222")));
    docs.add(doc12);

    Document doc13 = new Document();
    doc13.add(new SortedDocValuesField(nullField, new BytesRef(FieldValue.NULL_FIELD_VALUE)));
    docs.add(doc13);

    Document doc14 = new Document();
    doc14.add(
        new SortedSetDocValuesField(
            objectIdField, new BytesRef(new ObjectId("7".repeat(24)).toByteArray())));
    docs.add(doc14);

    Document doc15 = new Document();
    doc15.add(
        new SortedSetDocValuesField(
            objectIdField, new BytesRef(new ObjectId("F".repeat(24)).toByteArray())));
    docs.add(doc15);

    Document doc16 = new Document();
    doc16.add(
        new SortedSetDocValuesField(
            booleanField, new BytesRef(FieldValue.BOOLEAN_FALSE_FIELD_VALUE)));
    docs.add(doc16);

    Document doc17 = new Document();
    doc17.add(
        new SortedSetDocValuesField(
            booleanField, new BytesRef(FieldValue.BOOLEAN_TRUE_FIELD_VALUE)));
    docs.add(doc17);

    try (var writer = new RandomSegmentingIndexWriter(this.directory)) {
      writer.addDocuments(docs);
    }

    DirectoryReader reader = DirectoryReader.open(this.directory);
    this.searcher = new IndexSearcher(reader);
  }

  @After
  public void cleanup() throws IOException {
    this.searcher.getIndexReader().close();
    this.directory.close();
  }

  /** Create a single column sort over field 'f' with the given SortOptions. */
  private static Sort createSort(SortOptions options) {
    SortField[] sortFields = new SortField[1];
    sortFields[0] =
        new MqlMixedSort(new MongotSortField(FieldPath.newRoot("f"), options), Optional.empty());
    return new Sort(sortFields);
  }

  @Test
  public void testCompareBottom() throws IOException {
    Query query = new MatchAllDocsQuery();
    Sort sort = createSort(UserFieldSortOptions.DEFAULT_ASC);

    TopDocs topDocs = this.searcher.search(query, 16, sort);

    Object[] values = Arrays.stream(topDocs.scoreDocs).map(s -> ((FieldDoc) s).fields[0]).toArray();

    assertThat(values)
        .asList()
        .containsExactly(
            BsonUtils.MIN_KEY,
            BsonUtils.MIN_KEY,
            new BsonDouble(Double.NaN),
            new BsonDouble(0.0),
            new BsonInt64(1),
            new BsonDouble(2.0),
            new BsonInt64(3),
            new BsonString("1"),
            new BsonString("2"),
            new BsonBinary(UUID.fromString("11111111-1111-1111-1111-111111111111")),
            new BsonBinary(UUID.fromString("22222222-2222-2222-2222-222222222222")),
            new BsonObjectId(new ObjectId("7".repeat(24))),
            new BsonObjectId(new ObjectId("F".repeat(24))),
            BsonBoolean.FALSE,
            BsonBoolean.TRUE,
            new BsonDateTime(123456789))
        .inOrder();
  }

  @Test
  public void testCompareTop() throws IOException {
    Sort sort = createSort(UserFieldSortOptions.DEFAULT_ASC);
    Query query = new MatchAllDocsQuery();

    ScoreDoc after = new FieldDoc(-1, 1f, new Object[] {new BsonDouble(0.0)});
    TopDocs topDocs = this.searcher.searchAfter(after, query, 15, sort);

    Object[] values = Arrays.stream(topDocs.scoreDocs).map(s -> ((FieldDoc) s).fields[0]).toArray();

    assertThat(values)
        .asList()
        .containsExactly(
            new BsonDouble(0.0),
            new BsonInt64(1),
            new BsonDouble(2.0),
            new BsonInt64(3),
            new BsonString("1"),
            new BsonString("2"),
            new BsonBinary(UUID.fromString("11111111-1111-1111-1111-111111111111")),
            new BsonBinary(UUID.fromString("22222222-2222-2222-2222-222222222222")),
            new BsonObjectId(new ObjectId("7".repeat(24))),
            new BsonObjectId(new ObjectId("F".repeat(24))),
            BsonBoolean.FALSE,
            BsonBoolean.TRUE,
            new BsonDateTime(123456789),
            new BsonDateTime(923456789),
            new BsonDateTime(Integer.MAX_VALUE))
        .inOrder();
  }

  @Test
  public void testCompareBottomDesc() throws IOException {
    Sort sort = createSort(UserFieldSortOptions.DEFAULT_DESC);
    Query query = new MatchAllDocsQuery();

    TopDocs topDocs = this.searcher.search(query, 18, sort);

    Object[] values = Arrays.stream(topDocs.scoreDocs).map(s -> ((FieldDoc) s).fields[0]).toArray();

    assertThat(values)
        .asList()
        .containsExactly(
            new BsonDateTime(Integer.MAX_VALUE),
            new BsonDateTime(923456789),
            new BsonDateTime(123456789),
            BsonBoolean.TRUE,
            BsonBoolean.FALSE,
            new BsonObjectId(new ObjectId("F".repeat(24))),
            new BsonObjectId(new ObjectId("7".repeat(24))),
            new BsonBinary(UUID.fromString("22222222-2222-2222-2222-222222222222")),
            new BsonBinary(UUID.fromString("11111111-1111-1111-1111-111111111111")),
            new BsonString("2"),
            new BsonString("1"),
            new BsonInt64(3),
            new BsonDouble(2.0),
            new BsonInt64(1),
            new BsonDouble(0.0),
            new BsonDouble(Double.NaN),
            BsonUtils.MIN_KEY,
            BsonUtils.MIN_KEY)
        .inOrder();
  }

  @Test
  public void testCompareTopDesc() throws IOException {
    Sort sort = createSort(UserFieldSortOptions.DEFAULT_DESC);
    Query query = new MatchAllDocsQuery();
    ScoreDoc after = new FieldDoc(-1, 1f, new Object[] {new BsonDateTime(123456789)});

    TopDocs topDocs = this.searcher.searchAfter(after, query, 16, sort);

    Object[] values = Arrays.stream(topDocs.scoreDocs).map(s -> ((FieldDoc) s).fields[0]).toArray();

    assertThat(values)
        .asList()
        .containsExactly(
            new BsonDateTime(123456789),
            BsonBoolean.TRUE,
            BsonBoolean.FALSE,
            new BsonObjectId(new ObjectId("F".repeat(24))),
            new BsonObjectId(new ObjectId("7".repeat(24))),
            new BsonBinary(UUID.fromString("22222222-2222-2222-2222-222222222222")),
            new BsonBinary(UUID.fromString("11111111-1111-1111-1111-111111111111")),
            new BsonString("2"),
            new BsonString("1"),
            new BsonInt64(3),
            new BsonDouble(2.0),
            new BsonInt64(1),
            new BsonDouble(0.0),
            new BsonDouble(Double.NaN),
            BsonUtils.MIN_KEY,
            BsonUtils.MIN_KEY)
        .inOrder();
  }
}
