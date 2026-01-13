package com.xgen.mongot.index.lucene.query.sort.mixed;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Iterables;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.field.FieldValue;
import com.xgen.mongot.index.query.sort.NullEmptySortPosition;
import com.xgen.mongot.index.query.sort.UserFieldSortOptions;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.util.BytesRef;
import org.bson.BsonNull;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MixedNullLeafComparatorTest {

  private static final String FIELD = "field";

  private ByteBuffersDirectory directory;
  private SortedDocValues dv;

  @Before
  public void setUp() throws Exception {
    this.directory = new ByteBuffersDirectory();
    IndexWriter writer = new IndexWriter(this.directory, new IndexWriterConfig());
    writer.addDocuments(
        List.of(
            List.of(new SortedDocValuesField(FIELD, new BytesRef(FieldValue.NULL_FIELD_VALUE))),
            List.of(new SortedDocValuesField(FIELD, new BytesRef(FieldValue.NULL_FIELD_VALUE))),
            List.of(new NumericDocValuesField("OtherField", 5))));
    writer.commit();
    writer.close();
    DirectoryReader reader = DirectoryReader.open(this.directory);
    this.dv = DocValues.getSorted(Iterables.getOnlyElement(reader.leaves()).reader(), FIELD);
  }

  private MixedNullLeafComparator createComparator(Optional<BsonValue> top) {
    MixedFieldComparator fieldComparator =
        new MixedFieldComparator(
            FieldName.TypeField.NULL, BsonType.NULL, FieldPath.parse(FIELD), Optional.empty());

    int unused = 5;
    CompositeComparator compositeComparator =
        CompositeComparator.create(
            new MixedFieldComparator[] {fieldComparator}, UserFieldSortOptions.DEFAULT_ASC, unused);

    top.ifPresent(compositeComparator::setTopValue);
    return new MixedNullLeafComparator(
        this.dv,
        SortUtil.getBracketPriority(BsonType.NULL, NullEmptySortPosition.LOWEST),
        BsonUtils.MIN_KEY);
  }

  @After
  public void tearDown() throws Exception {
    this.directory.close();
  }

  @Test
  public void hasValue() throws IOException {
    var comparator = createComparator(Optional.empty());

    assertTrue(comparator.hasValue(0));
    assertEquals(BsonUtils.MIN_KEY, comparator.getCurrentValue());

    assertTrue(comparator.hasValue(1));
    assertEquals(BsonUtils.MIN_KEY, comparator.getCurrentValue());

    assertFalse(comparator.hasValue(2));
    // getCurrentValue() is now undefined.
  }

  @Test
  public void hasValueWithTop() throws IOException {
    var comparator = createComparator(Optional.of(BsonNull.VALUE));

    assertTrue(comparator.hasValue(0));
    assertEquals(BsonUtils.MIN_KEY, comparator.getCurrentValue());

    assertTrue(comparator.hasValue(1));
    assertEquals(BsonUtils.MIN_KEY, comparator.getCurrentValue());

    assertFalse(comparator.hasValue(2));
    // getCurrentValue() is now undefined.
  }

  @Test
  public void compareTopExactMatch() throws IOException {
    var comparator = createComparator(Optional.of(BsonNull.VALUE));

    assertTrue(comparator.hasValue(0)); // 'N' for null
    assertThat(comparator.compareTopToCurrent()).isEqualTo(0);

    assertTrue(comparator.hasValue(1)); // 'N' for null
    assertThat(comparator.compareTopToCurrent()).isEqualTo(0);
  }

  @Test
  public void compareBottomExactMatch() throws IOException {
    var comparator = createComparator(Optional.of(BsonUtils.MAX_KEY));
    comparator.notifyNewBottom(BsonNull.VALUE);

    assertTrue(comparator.hasValue(0)); // 'N' for null
    assertThat(comparator.compareBottomToCurrent()).isEqualTo(0);

    assertTrue(comparator.hasValue(1)); // 'N' for null
    assertThat(comparator.compareBottomToCurrent()).isEqualTo(0);
  }
}
