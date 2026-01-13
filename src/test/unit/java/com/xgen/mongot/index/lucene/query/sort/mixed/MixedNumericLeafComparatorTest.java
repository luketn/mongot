package com.xgen.mongot.index.lucene.query.sort.mixed;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import com.google.common.collect.Iterables;
import com.xgen.mongot.index.query.sort.NullEmptySortPosition;
import com.xgen.mongot.util.BsonUtils;
import java.io.IOException;
import java.util.List;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.tests.index.AssertingDirectoryReader;
import org.apache.lucene.util.BytesRef;
import org.bson.BsonDouble;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class MixedNumericLeafComparatorTest {

  private static final String FIELD = "field";

  private ByteBuffersDirectory directory;
  private NumericDocValues dv;

  @Before
  public void setUp() throws Exception {
    this.directory = new ByteBuffersDirectory();
    IndexWriter writer = new IndexWriter(this.directory, new IndexWriterConfig());
    writer.addDocuments(
        List.of(
            List.of(new NumericDocValuesField(FIELD, 1)),
            List.of(new NumericDocValuesField(FIELD, 2)),
            List.of(new NumericDocValuesField(FIELD, 3)),
            List.of(new SortedDocValuesField("OtherField", new BytesRef("5")))));
    writer.commit();
    writer.close();
    DirectoryReader reader = new AssertingDirectoryReader(DirectoryReader.open(this.directory));
    this.dv = DocValues.getNumeric(Iterables.getOnlyElement(reader.leaves()).reader(), FIELD);
  }

  private MixedLongLeafComparator createComparator(BsonValue top) {
    CompositeComparator mockComparator = Mockito.mock(CompositeComparator.class);
    when(mockComparator.getTop()).thenReturn(top);
    when(mockComparator.getTopBracket())
        .thenReturn(SortUtil.getBracketPriority(top.getBsonType(), NullEmptySortPosition.LOWEST));

    return new MixedLongLeafComparator(mockComparator, this.dv);
  }

  @After
  public void tearDown() throws Exception {
    this.directory.close();
  }

  @Test
  public void hasValue() throws IOException {
    var comparator = createComparator(BsonNull.VALUE);

    assertTrue(comparator.hasValue(0));
    assertEquals(new BsonInt64(1), comparator.getCurrentValue());

    assertTrue(comparator.hasValue(1));
    assertEquals(new BsonInt64(2), comparator.getCurrentValue());

    assertTrue(comparator.hasValue(2));
    assertEquals(new BsonInt64(3), comparator.getCurrentValue());

    assertFalse(comparator.hasValue(3));
    // getCurrentValue() is now undefined.
  }

  @Test
  public void compareTopLong() throws IOException {
    var comparator = createComparator(new BsonInt64(2));

    assertTrue(comparator.hasValue(0)); // 1
    assertThat(comparator.compareTopToCurrent()).isGreaterThan(0);

    assertTrue(comparator.hasValue(1)); // 2
    assertThat(comparator.compareTopToCurrent()).isEqualTo(0);

    assertTrue(comparator.hasValue(2)); // 3
    assertThat(comparator.compareTopToCurrent()).isLessThan(0);
  }

  @Test
  public void compareTopDouble() throws IOException {
    var comparator = createComparator(new BsonDouble(2.0));

    assertTrue(comparator.hasValue(0)); // 1
    assertThat(comparator.compareTopToCurrent()).isGreaterThan(0);

    assertTrue(comparator.hasValue(1)); // 2
    assertThat(comparator.compareTopToCurrent()).isEqualTo(0);

    assertTrue(comparator.hasValue(2)); // 3
    assertThat(comparator.compareTopToCurrent()).isLessThan(0);
  }

  @Test
  public void compareTopNaN() throws IOException {
    var comparator = createComparator(new BsonDouble(Double.NaN));

    assertTrue(comparator.hasValue(0)); // 1
    assertThat(comparator.compareTopToCurrent()).isLessThan(0);
  }

  @Test
  public void compareBottomExactMatch() throws IOException {
    var comparator = createComparator(BsonUtils.MAX_KEY);
    comparator.notifyNewBottom(new BsonInt64(2));

    assertTrue(comparator.hasValue(0)); // 1
    assertThat(comparator.compareBottomToCurrent()).isGreaterThan(0);

    assertTrue(comparator.hasValue(1)); // 2
    assertThat(comparator.compareBottomToCurrent()).isEqualTo(0);

    assertTrue(comparator.hasValue(2)); // 3
    assertThat(comparator.compareBottomToCurrent()).isLessThan(0);
  }

  @Test
  public void compareBottomNoMatch() throws IOException {
    var comparator = createComparator(BsonUtils.MAX_KEY);
    comparator.notifyNewBottom(new BsonDouble(2.7182));

    assertTrue(comparator.hasValue(1)); // 2
    assertThat(comparator.compareBottomToCurrent()).isGreaterThan(0);

    assertTrue(comparator.hasValue(2)); // 3
    assertThat(comparator.compareBottomToCurrent()).isLessThan(0);
  }
}
