package com.xgen.mongot.index.lucene.query;

import static com.xgen.mongot.index.lucene.query.MoreLikeThisTestUtil.buildBooleanQuery;
import static com.xgen.mongot.index.lucene.query.MoreLikeThisTestUtil.compareBQs;
import static com.xgen.mongot.index.lucene.query.MoreLikeThisTestUtil.termQuery;
import static com.xgen.mongot.index.lucene.query.MoreLikeThisTestUtil.termQueryForMulti;
import static com.xgen.mongot.index.lucene.query.MoreLikeThisTestUtil.withLuceneMultiField;
import static com.xgen.mongot.index.lucene.query.MoreLikeThisTestUtil.withLuceneStringFields;

import com.xgen.mongot.index.definition.DocumentFieldDefinition;
import com.xgen.mongot.index.definition.StringFieldDefinition;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.StringFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.query.operators.OperatorBuilder;
import java.util.List;
import java.util.Map;
import org.apache.lucene.search.BooleanQuery;
import org.bson.BsonDocument;
import org.junit.Test;

public class MoreLikeThisQueryFactoryTest {
  private static final DocumentFieldDefinition MAPPING =
      DocumentFieldDefinitionBuilder.builder()
          .dynamic(false)
          .field(
              "a",
              FieldDefinitionBuilder.builder()
                  .string(StringFieldDefinitionBuilder.builder().build())
                  .build())
          .build();

  private static final DocumentFieldDefinition DYNAMIC_MAPPING =
      DocumentFieldDefinitionBuilder.builder().dynamic(true).build();

  // Mapping with multi - analyzer specified.
  private static final StringFieldDefinition FIELD_WITH_MULTI =
      StringFieldDefinitionBuilder.builder()
          .multi(
              "kw", StringFieldDefinitionBuilder.builder().analyzerName("lucene.keyword").build())
          .build();

  private static final DocumentFieldDefinition MAPPING_WITH_MULTI_1 =
      DocumentFieldDefinitionBuilder.builder()
          .dynamic(false)
          .field("a", FieldDefinitionBuilder.builder().string(FIELD_WITH_MULTI).build())
          .build();

  // Mapping with multi - only search analyzer specified.
  private static final StringFieldDefinition FIELD_WITH_MULTI_WITH_SEARCH_ANALYZER =
      StringFieldDefinitionBuilder.builder()
          .multi(
              "kw",
              StringFieldDefinitionBuilder.builder().searchAnalyzerName("lucene.keyword").build())
          .build();

  private static final DocumentFieldDefinition MAPPING_WITH_MULTI_2 =
      DocumentFieldDefinitionBuilder.builder()
          .dynamic(false)
          .field(
              "a",
              FieldDefinitionBuilder.builder()
                  .string(FIELD_WITH_MULTI_WITH_SEARCH_ANALYZER)
                  .build())
          .build();

  /**
   * MLT won't pick a term to search for if it's not in at least two documents. So for all the tests
   * here we need to duplicate the documents we insert as a way to deal with this.
   */
  List<LuceneSearchTranslation.TestDocument> duplicateDocument(
      LuceneSearchTranslation.TestDocument doc) {
    return List.of(doc, doc);
  }

  @Test
  public void testSimpleDoc() throws Exception {
    var operator =
        OperatorBuilder.moreLikeThis().like(BsonDocument.parse("{a: \"hello there\"}")).build();

    var expected = buildBooleanQuery(termQuery("a", "hello"), termQuery("a", "there"));

    Map<String, String> fields = withLuceneStringFields(Map.of("a", "hello there"));
    var doc = new LuceneSearchTranslation.TestDocument(fields, Map.of());
    var actual =
        LuceneSearchTranslation.get()
            .translateWithIndexedDocuments(operator, duplicateDocument(doc));
    compareBQs(expected, actual);
  }

  @Test
  public void testHasStopword() throws Exception {
    var operator =
        OperatorBuilder.moreLikeThis().like(BsonDocument.parse("{a: \"hello the\"}")).build();

    var expected = buildBooleanQuery(termQuery("a", "hello"), termQuery("a", "the"));

    LuceneSearchTranslation.TestDocument doc =
        new LuceneSearchTranslation.TestDocument(
            withLuceneStringFields(Map.of("a", "hello the")), Map.of());
    var actual =
        LuceneSearchTranslation.get()
            .translateWithIndexedDocuments(operator, duplicateDocument(doc));
    compareBQs(expected, actual);
  }

  @Test
  public void testDocWithRepeatingFields() throws Exception {
    // Not sure if valid, can't create such an object from mongosh, but doesn't hurt to see
    // what happens.
    var operator =
        OperatorBuilder.moreLikeThis()
            .like(BsonDocument.parse("{a: 'hello there', a: 'mongo'}"))
            .build();

    var expected = buildBooleanQuery(termQuery("a", "mongo"));

    // We need to have these terms indexed, Lucene's MLT will not pick up anything to be searched
    // if it's not present in the index.
    LuceneSearchTranslation.TestDocument doc =
        new LuceneSearchTranslation.TestDocument(
            withLuceneStringFields(Map.of("a", "mongo")), Map.of());
    var actual =
        LuceneSearchTranslation.get()
            .translateWithIndexedDocuments(operator, duplicateDocument(doc));
    compareBQs(expected, actual);
  }

  @Test
  public void testMoreLikeThisOnIndexWithMulti() throws Exception {
    var operator =
        OperatorBuilder.moreLikeThis().like(BsonDocument.parse("{a: \"hello there\"}")).build();

    // Note how we searched for the term that was indexed by the multi, which is "hello world".
    var expected =
        buildBooleanQuery(
            termQuery("a", "hello"),
            termQuery("a", "there"),
            termQueryForMulti("a", "kw", "hello there"));

    Map<String, String> stringFields = withLuceneStringFields(Map.of("a", "hello there"));
    Map<String, String> fieldsForMulti = withLuceneMultiField("a", "kw", "hello there");
    var doc = new LuceneSearchTranslation.TestDocument(stringFields, fieldsForMulti);
    var actual =
        LuceneSearchTranslation.mapped(MAPPING_WITH_MULTI_1)
            .translateWithIndexedDocuments(operator, duplicateDocument(doc));
    compareBQs(expected, actual);
  }

  @Test
  public void testMoreLikeThisOnIndexWithMultiWithSearchAnalyzer() throws Exception {
    var operator =
        OperatorBuilder.moreLikeThis().like(BsonDocument.parse("{a: \"hello there\"}")).build();

    // Note how we searched for the term that was indexed by the multi, which is "hello world".
    var expected =
        buildBooleanQuery(
            termQuery("a", "hello"),
            termQuery("a", "there"),
            termQueryForMulti("a", "kw", "hello there"));

    Map<String, String> stringFields = withLuceneStringFields(Map.of("a", "hello there"));
    Map<String, String> fieldsForMulti = withLuceneMultiField("a", "kw", "hello there");
    var doc = new LuceneSearchTranslation.TestDocument(stringFields, fieldsForMulti);
    var actual =
        LuceneSearchTranslation.mapped(MAPPING_WITH_MULTI_2)
            .translateWithIndexedDocuments(operator, duplicateDocument(doc));
    compareBQs(expected, actual);
  }

  @Test
  public void testDuplicatePaths() throws Exception {
    var operator =
        OperatorBuilder.moreLikeThis()
            .like(BsonDocument.parse("{a: \"hello there\", a: \"hi\"}"))
            .build();

    var expected = buildBooleanQuery(termQuery("a", "hi"));
    var indexedDoc =
        new LuceneSearchTranslation.TestDocument(
            withLuceneStringFields(Map.of("a", "hi")), Map.of());
    compareBQs(
        expected,
        LuceneSearchTranslation.get()
            .translateWithIndexedDocuments(operator, duplicateDocument(indexedDoc)));
  }

  @Test
  public void testNumbersIgnored() throws Exception {
    var operator =
        OperatorBuilder.moreLikeThis()
            .like(BsonDocument.parse("{a: \"hello there\", b:5}"))
            .build();

    var expected = buildBooleanQuery(termQuery("a", "hello"), termQuery("a", "there"));

    var doc =
        new LuceneSearchTranslation.TestDocument(
            withLuceneStringFields(Map.of("a", "hello there")), Map.of());
    compareBQs(
        expected,
        LuceneSearchTranslation.get()
            .translateWithIndexedDocuments(operator, duplicateDocument(doc)));
  }

  @Test
  public void testNestedDoc() throws Exception {
    var operator =
        OperatorBuilder.moreLikeThis()
            .like(BsonDocument.parse("{a: \"hello there\", b: { c: \"mongo db\" } }"))
            .build();

    var expected =
        buildBooleanQuery(
            termQuery("a", "hello"),
            termQuery("a", "there"),
            termQuery("b.c", "mongo"),
            termQuery("b.c", "db"));

    var indexedDoc =
        new LuceneSearchTranslation.TestDocument(
            withLuceneStringFields(Map.of("a", "hello there", "b.c", "mongo db")), Map.of());
    compareBQs(
        expected,
        LuceneSearchTranslation.get()
            .translateWithIndexedDocuments(operator, duplicateDocument(indexedDoc)));
  }

  @Test
  public void testLikeContainsNotIndexedField() throws Exception {
    var operator =
        OperatorBuilder.moreLikeThis()
            .like(BsonDocument.parse("{a: \"hello there\", b: \"mongo db\", c: 5}"))
            .build();

    var expected = buildBooleanQuery(termQuery("a", "hello"), termQuery("a", "there"));

    var indexedDoc =
        new LuceneSearchTranslation.TestDocument(
            withLuceneStringFields(Map.of("a", "hello there", "b.c", "mongo db")), Map.of());

    compareBQs(
        expected,
        LuceneSearchTranslation.mapped(MAPPING)
            .translateWithIndexedDocuments(operator, duplicateDocument(indexedDoc)));
  }

  @Test
  public void testLikeContainsArray() throws Exception {
    var operator =
        OperatorBuilder.moreLikeThis()
            .like(BsonDocument.parse("{priority: ['P1', ['P2', 'P3 P4']] }"))
            .build();
    BooleanQuery expected =
        buildBooleanQuery(
            termQuery("priority", "p1"),
            termQuery("priority", "p2"),
            termQuery("priority", "p3"),
            termQuery("priority", "p4"));

    var indexedDoc =
        new LuceneSearchTranslation.TestDocument(
            withLuceneStringFields(Map.of("priority", "p1 p2 p3 p4")), Map.of());
    compareBQs(
        expected,
        LuceneSearchTranslation.mapped(DYNAMIC_MAPPING)
            .translateWithIndexedDocuments(operator, duplicateDocument(indexedDoc)));
  }

  @Test
  public void testLikeContainsArrayOfNestedDocs() throws Exception {
    var book1 = "{title: 'Mongo', author: ['Dev', 'Eliot Horowitz' ]}";
    var book2 = "{title: 'Java', author: 'James'}";
    var docString = String.format("{books: [%s, %s]}", book1, book2);
    var operator = OperatorBuilder.moreLikeThis().like(BsonDocument.parse(docString)).build();
    var expected =
        buildBooleanQuery(
            termQuery("books.title", "mongo"),
            termQuery("books.author", "dev"),
            termQuery("books.author", "eliot"),
            termQuery("books.author", "horowitz"),
            termQuery("books.title", "java"),
            termQuery("books.author", "james"));

    var doc =
        new LuceneSearchTranslation.TestDocument(
            withLuceneStringFields(
                Map.of(
                    "books.title", "Mongo and Java", "books.author", "Eliot Horowitz Dev James")),
            Map.of());
    var query =
        LuceneSearchTranslation.mapped(DYNAMIC_MAPPING)
            .translateWithIndexedDocuments(operator, duplicateDocument(doc));
    compareBQs(expected, query);
  }
}
