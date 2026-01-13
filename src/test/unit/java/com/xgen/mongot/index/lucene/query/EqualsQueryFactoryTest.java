package com.xgen.mongot.index.lucene.query;

import com.xgen.mongot.index.definition.DocumentFieldDefinition;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.points.DatePoint;
import com.xgen.mongot.index.query.points.DoublePoint;
import com.xgen.mongot.index.query.points.LongPoint;
import com.xgen.mongot.index.query.points.NumericPoint;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.TokenFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.query.operators.OperatorBuilder;
import java.util.Calendar;
import java.util.Optional;
import java.util.TimeZone;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;

public class EqualsQueryFactoryTest {
  @Test
  public void testBoolean() throws Exception {
    var operator = OperatorBuilder.equals().path("a").value(true).build();
    var expected = new ConstantScoreQuery(new TermQuery(new Term("$type:boolean/a", "T")));
    LuceneSearchTranslation.get().assertTranslatedTo(operator, expected);
  }

  @Test
  public void testObjectId() throws Exception {
    var objectId = new ObjectId("507f1f77bcf86cd799439011");
    var operator = OperatorBuilder.equals().path("a").value(objectId).build();
    var expected =
        new ConstantScoreQuery(
            new TermQuery(new Term("$type:objectId/a", new BytesRef(objectId.toByteArray()))));
    LuceneSearchTranslation.get().assertTranslatedTo(operator, expected);
  }

  @Test
  public void testLong() throws Exception {
    Optional<NumericPoint> value = Optional.of(new LongPoint(2L));
    var operator = OperatorBuilder.equals().path("a").value(2).build();
    var expectedRangeOperator =
        OperatorBuilder.range().path("a").numericBounds(value, value, true, true).build();
    // Check that an equals query for n translates to a range query for [n, n]
    LuceneSearchTranslation.get()
        .assertTranslatedTo(
            operator, LuceneSearchTranslation.get().translate(expectedRangeOperator));
  }

  @Test
  public void testDouble() throws Exception {
    Optional<NumericPoint> value = Optional.of(new DoublePoint(2.01));
    var operator = OperatorBuilder.equals().path("a").value(2.01).build();
    var expectedRangeOperator =
        OperatorBuilder.range().path("a").numericBounds(value, value, true, true).build();
    // Check that an equals query for n translates to a range query for [n, n]
    LuceneSearchTranslation.get()
        .assertTranslatedTo(
            operator, LuceneSearchTranslation.get().translate(expectedRangeOperator));
  }

  @Test
  public void testDate() throws Exception {
    var date =
        new Calendar.Builder()
            .setDate(2022, Calendar.DECEMBER, 6)
            .setTimeOfDay(13, 13, 13)
            .setTimeZone(TimeZone.getTimeZone("UTC"))
            .build()
            .getTime();
    var datePoint = Optional.of(new DatePoint(date));
    var operator = OperatorBuilder.equals().path("a").value(date).build();
    var expectedRangeOperator =
        OperatorBuilder.range().path("a").dateBounds(datePoint, datePoint, true, true).build();
    // Check that an equals query for n translates to a range query for [n, n]
    LuceneSearchTranslation.get()
        .assertTranslatedTo(
            operator, LuceneSearchTranslation.get().translate(expectedRangeOperator));
  }

  @Test
  public void testString() throws Exception {
    var string = "example";
    var operator = OperatorBuilder.equals().path("a").value(string).build();

    var indexQuery =
        new ConstantScoreQuery(new TermQuery(new Term("$type:token/a", new BytesRef("example"))));
    var docValuesQuery =
        SortedSetDocValuesField.newSlowExactQuery("$type:token/a", new BytesRef("example"));

    var expectedOperator = new IndexOrDocValuesQuery(indexQuery, docValuesQuery);

    DocumentFieldDefinition mappings =
        DocumentFieldDefinitionBuilder.builder()
            .field(
                "a",
                FieldDefinitionBuilder.builder()
                    .token(TokenFieldDefinitionBuilder.builder().build())
                    .build())
            .build();

    LuceneSearchTranslation.mapped(mappings).assertTranslatedTo(operator, expectedOperator);
  }

  @Test
  public void testStringIsNotIndexedAsTokenThrowsException() throws Exception {
    var string = "multi token input";
    var operator = OperatorBuilder.equals().path("a").value(string).build();
    Assert.assertThrows(
        InvalidQueryException.class, () -> LuceneSearchTranslation.get().translate(operator));
  }

  @Test
  public void testStringIsNotIndexedAsTokenDoesNotThrowException() throws Exception {
    var string = "singletokeninput";
    var operator = OperatorBuilder.equals().path("a").value(string).build();
    var query = LuceneSearchTranslation.get().translate(operator);
    Assert.assertNotNull(query); // known issue: inconsistent with multi-token behavior above
  }

  @Test
  public void testUuid() throws Exception {
    var uuidString = "00000000-1111-2222-3333-444444444444";
    var operator = OperatorBuilder.equals().path("a").uuidValue(uuidString).build();

    String uuidField = "$type:uuid/a";
    var indexQuery =
        new ConstantScoreQuery(new TermQuery(new Term(uuidField, new BytesRef(uuidString))));
    var docValuesQuery =
        SortedSetDocValuesField.newSlowExactQuery(uuidField, new BytesRef(uuidString));

    var expectedOperator = new IndexOrDocValuesQuery(indexQuery, docValuesQuery);
    LuceneSearchTranslation.get().assertTranslatedTo(operator, expectedOperator);
  }
}
