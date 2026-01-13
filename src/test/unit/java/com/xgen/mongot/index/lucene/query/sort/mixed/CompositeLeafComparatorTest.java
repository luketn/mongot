package com.xgen.mongot.index.lucene.query.sort.mixed;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import com.xgen.mongot.index.lucene.field.FieldName.TypeField;
import com.xgen.mongot.index.lucene.field.FieldValue;
import com.xgen.mongot.index.query.sort.NullEmptySortPosition;
import com.xgen.mongot.index.query.sort.SortOrder;
import com.xgen.mongot.index.query.sort.UserFieldSortOptions;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.LuceneIndexRule;
import java.io.IOException;
import java.util.Optional;
import java.util.random.RandomGenerator;
import java.util.random.RandomGeneratorFactory;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.util.BytesRef;
import org.bson.BsonBoolean;
import org.bson.BsonInt64;
import org.bson.BsonMinKey;
import org.bson.BsonNull;
import org.bson.BsonType;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.FromDataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

@RunWith(Theories.class)
public class CompositeLeafComparatorTest {

  @ClassRule public static final LuceneIndexRule luceneIndexRule = new LuceneIndexRule();

  private static final RandomGenerator rng = RandomGeneratorFactory.getDefault().create();

  private final MixedFieldComparator intComparator =
      new MixedFieldComparator(
          TypeField.NUMBER_INT64_V2,
          BsonType.INT64,
          FieldPath.newRoot("intOrNull"),
          Optional.empty());

  private final MixedFieldComparator nullComparator =
      new MixedFieldComparator(
          TypeField.NULL, BsonType.NULL, FieldPath.newRoot("intOrNull"), Optional.empty());

  private final MixedFieldComparator emptyComparator =
      new MixedFieldComparator(
          TypeField.NULL, BsonType.NULL, FieldPath.newRoot("doesNotExist"), Optional.empty());

  public static Document createDoc() {
    Document doc = new Document();
    if (rng.nextBoolean()) {
      doc.add(
          new LongField(
              TypeField.NUMBER_INT64_V2.getLuceneFieldName(
                  FieldPath.newRoot("intOrNull"), Optional.empty()),
              rng.nextInt(),
              Store.NO));
    } else {
      doc.add(
          new StringField(
              TypeField.NULL.getLuceneFieldName(FieldPath.newRoot("intOrNull"), Optional.empty()),
              FieldValue.NULL_FIELD_VALUE,
              Store.NO));
      doc.add(
          new SortedDocValuesField(
              TypeField.NULL.getLuceneFieldName(FieldPath.newRoot("intOrNull"), Optional.empty()),
              new BytesRef(FieldValue.NULL_FIELD_VALUE)));
    }

    return doc;
  }

  private static final UserFieldSortOptions ASC_NULL_HIGHEST =
      new UserFieldSortOptions(SortOrder.ASC, NullEmptySortPosition.HIGHEST);
  private static final UserFieldSortOptions DESC_NULL_HIGHEST =
      new UserFieldSortOptions(SortOrder.DESC, NullEmptySortPosition.HIGHEST);

  @DataPoints("all")
  public static final ImmutableList<UserFieldSortOptions> allSort =
      ImmutableList.of(
          UserFieldSortOptions.DEFAULT_ASC,
          UserFieldSortOptions.DEFAULT_DESC,
          ASC_NULL_HIGHEST,
          DESC_NULL_HIGHEST);

  @DataPoints("sortNullsToFront")
  public static final ImmutableList<UserFieldSortOptions> sortNullsToFront =
      ImmutableList.of(UserFieldSortOptions.DEFAULT_ASC, DESC_NULL_HIGHEST);

  @BeforeClass
  public static void setup() {
    for (int i = 0; i < 1_000; ++i) {
      luceneIndexRule.add(createDoc());
    }
  }

  @Theory
  public void competitiveIterator_noTopBottom_returnsAllDocs(
      @FromDataPoints("all") UserFieldSortOptions sort) throws IOException {
    int numHits = 1;
    var leafContext = luceneIndexRule.getIndexReader().leaves().getFirst();
    CompositeComparator compositeComparator =
        CompositeComparator.create(
            new MixedFieldComparator[] {this.nullComparator, this.intComparator}, sort, numHits);

    CompositeLeafComparator leafComparator =
        (CompositeLeafComparator) compositeComparator.getLeafComparator(leafContext);
    compositeComparator.setSingleSort();

    assertEquals(leafContext.reader().numDocs(), leafComparator.competitiveIterator().cost());
    leafComparator.setHitsThresholdReached();
    assertEquals(leafContext.reader().numDocs(), leafComparator.competitiveIterator().cost());
  }

  @Theory
  public void competitiveIterator_topSetSameBracket_returnsAllDocs(
      @FromDataPoints("all") UserFieldSortOptions sort) throws IOException {
    int numHits = 1;
    var leafContext = luceneIndexRule.getIndexReader().leaves().getFirst();
    CompositeComparator compositeComparator =
        CompositeComparator.create(
            new MixedFieldComparator[] {this.nullComparator, this.intComparator}, sort, numHits);
    compositeComparator.setSingleSort();
    compositeComparator.setTopValue(new BsonInt64(0));

    CompositeLeafComparator leafComparator =
        (CompositeLeafComparator) compositeComparator.getLeafComparator(leafContext);
    leafComparator.setHitsThresholdReached();

    assertEquals(leafContext.reader().numDocs(), leafComparator.competitiveIterator().cost());
  }

  @Theory
  public void competitiveIterator_topSetHighBracket_returnsAllDocs(
      @FromDataPoints("all") UserFieldSortOptions sort) throws IOException {
    int numHits = 1;
    var leafContext = luceneIndexRule.getIndexReader().leaves().getFirst();
    CompositeComparator compositeComparator =
        CompositeComparator.create(
            new MixedFieldComparator[] {this.nullComparator, this.intComparator}, sort, numHits);
    compositeComparator.setSingleSort();
    compositeComparator.setTopValue(BsonBoolean.valueOf(true));

    CompositeLeafComparator leafComparator =
        (CompositeLeafComparator) compositeComparator.getLeafComparator(leafContext);
    leafComparator.setHitsThresholdReached();

    assertEquals(leafContext.reader().numDocs(), leafComparator.competitiveIterator().cost());
  }

  @Theory
  public void competitiveIterator_bottomMissing_returnsNoDocs(
      @FromDataPoints("sortNullsToFront") UserFieldSortOptions sort) throws IOException {
    int numHits = 1;
    var leafContext = luceneIndexRule.getIndexReader().leaves().getFirst();
    CompositeComparator compositeComparator =
        CompositeComparator.create(
            new MixedFieldComparator[] {this.nullComparator, this.intComparator}, sort, numHits);
    compositeComparator.setSingleSort();

    CompositeLeafComparator leafComparator =
        (CompositeLeafComparator) compositeComparator.getLeafComparator(leafContext);
    leafComparator.setHitsThresholdReached();
    leafComparator.informBottom(new BsonNull());

    assertEquals(0, leafComparator.competitiveIterator().cost());
  }

  @Test
  public void competitiveIterator_bottomMinKeyNullsLast_returnsAllDocs() throws IOException {
    int numHits = 1;
    var leafContext = luceneIndexRule.getIndexReader().leaves().getFirst();
    CompositeComparator compositeComparator =
        CompositeComparator.create(
            new MixedFieldComparator[] {this.nullComparator, this.intComparator},
            ASC_NULL_HIGHEST,
            numHits);
    compositeComparator.setSingleSort();

    CompositeLeafComparator leafComparator =
        (CompositeLeafComparator) compositeComparator.getLeafComparator(leafContext);
    leafComparator.setHitsThresholdReached();
    leafComparator.informBottom(new BsonMinKey());

    // Legal to return 0 docs here, but not yet implemented
    assertEquals(leafContext.reader().numDocs(), leafComparator.competitiveIterator().cost());
  }

  @Test
  public void competitiveIterator_bottomHighDescSort_returnsAllDocs() throws IOException {
    int numHits = 1;
    var leafContext = luceneIndexRule.getIndexReader().leaves().getFirst();
    CompositeComparator compositeComparator =
        CompositeComparator.create(
            new MixedFieldComparator[] {this.nullComparator, this.intComparator},
            UserFieldSortOptions.DEFAULT_DESC,
            numHits);
    compositeComparator.setSingleSort();

    CompositeLeafComparator leafComparator =
        (CompositeLeafComparator) compositeComparator.getLeafComparator(leafContext);
    leafComparator.setHitsThresholdReached();
    leafComparator.informBottom(BsonBoolean.valueOf(true));

    // Legal to return 0 docs here, but not yet implemented
    assertEquals(leafContext.reader().numDocs(), leafComparator.competitiveIterator().cost());
  }

  @Test
  public void competitiveIterator_bottomInt64NullsLast_returnsAllDocs() throws IOException {
    int numHits = 1;
    var leafContext = luceneIndexRule.getIndexReader().leaves().getFirst();
    CompositeComparator compositeComparator =
        CompositeComparator.create(
            new MixedFieldComparator[] {this.nullComparator, this.intComparator},
            ASC_NULL_HIGHEST,
            numHits);
    compositeComparator.setSingleSort();

    CompositeLeafComparator leafComparator =
        (CompositeLeafComparator) compositeComparator.getLeafComparator(leafContext);
    leafComparator.setHitsThresholdReached();
    leafComparator.informBottom(new BsonInt64(Integer.MIN_VALUE));

    // Legal to return zero docs here, but requires full pruning
    assertEquals(leafContext.reader().numDocs(), leafComparator.competitiveIterator().cost());
  }

  @Theory
  public void competitiveIterator_nullBottomEmptyField_returnsNoDocs(
      @FromDataPoints("sortNullsToFront") UserFieldSortOptions sort) throws IOException {
    int numHits = 1;
    var leafContext = luceneIndexRule.getIndexReader().leaves().getFirst();
    CompositeComparator compositeComparator =
        CompositeComparator.create(
            new MixedFieldComparator[] {this.emptyComparator}, sort, numHits);
    compositeComparator.setSingleSort();

    CompositeLeafComparator leafComparator =
        (CompositeLeafComparator) compositeComparator.getLeafComparator(leafContext);
    leafComparator.setHitsThresholdReached();
    leafComparator.informBottom(new BsonNull());

    assertEquals(0, leafComparator.competitiveIterator().cost());
  }
}
