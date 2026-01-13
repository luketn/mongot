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
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.util.BytesRef;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class MixedOrdinalLeafComparatorTest {

  private static final String FIELD = "field";

  private ByteBuffersDirectory directory;
  private SortedDocValues dv;

  @Before
  public void setUp() throws Exception {
    // TODO(CLOUDP-280897): create FakeSortedDocValues class for lightweight tests?
    this.directory = new ByteBuffersDirectory();
    IndexWriter writer = new IndexWriter(this.directory, new IndexWriterConfig());
    writer.addDocuments(
        List.of(
            List.of(new SortedDocValuesField(FIELD, new BytesRef("a"))),
            List.of(new SortedDocValuesField(FIELD, new BytesRef("b"))),
            List.of(new SortedDocValuesField(FIELD, new BytesRef("c"))),
            List.of(new NumericDocValuesField("OtherField", 5))));
    writer.commit();
    writer.close();
    DirectoryReader reader = DirectoryReader.open(this.directory);
    this.dv = DocValues.getSorted(Iterables.getOnlyElement(reader.leaves()).reader(), FIELD);
  }

  private MixedOrdinalLeafComparator createComparator(BsonValue top) throws IOException {
    CompositeComparator mockComparator = Mockito.mock(CompositeComparator.class);
    when(mockComparator.getTop()).thenReturn(top);
    when(mockComparator.getTopBracket())
        .thenReturn(SortUtil.getBracketPriority(top.getBsonType(), NullEmptySortPosition.LOWEST));

    return new MixedOrdinalLeafComparator(
        mockComparator,
        this.dv,
        SortUtil.getBracketPriority(BsonType.STRING, NullEmptySortPosition.LOWEST),
        BsonUtils.STRING_CONVERTER);
  }

  @After
  public void tearDown() throws Exception {
    this.directory.close();
  }

  @Test
  public void hasValue() throws IOException {
    var comparator = createComparator(BsonNull.VALUE);

    assertTrue(comparator.hasValue(0));
    assertEquals(new BsonString("a"), comparator.getCurrentValue());

    assertTrue(comparator.hasValue(1));
    assertEquals(new BsonString("b"), comparator.getCurrentValue());

    assertTrue(comparator.hasValue(2));
    assertEquals(new BsonString("c"), comparator.getCurrentValue());

    assertFalse(comparator.hasValue(3));
    // getCurrentValue() is now undefined.
  }

  @Test
  public void compareTopExactMatch() throws IOException {
    var comparator = createComparator(new BsonString("b"));

    assertTrue(comparator.hasValue(0)); // 'a'
    assertThat(comparator.compareTopToCurrent()).isGreaterThan(0);

    assertTrue(comparator.hasValue(1)); // 'b'
    assertThat(comparator.compareTopToCurrent()).isEqualTo(0);

    assertTrue(comparator.hasValue(2)); // 'c'
    assertThat(comparator.compareTopToCurrent()).isLessThan(0);
  }

  @Test
  public void compareTopNoMatch() throws IOException {
    var comparator = createComparator(new BsonString("bb"));

    assertTrue(comparator.hasValue(0)); // 'a'
    assertThat(comparator.compareTopToCurrent()).isGreaterThan(0);

    assertTrue(comparator.hasValue(1)); // 'b'
    assertThat(comparator.compareTopToCurrent()).isGreaterThan(0);

    assertTrue(comparator.hasValue(2)); // 'c'
    assertThat(comparator.compareTopToCurrent()).isLessThan(0);
  }

  @Test
  public void compareBottomExactMatch() throws IOException {
    var comparator = createComparator(BsonUtils.MAX_KEY);
    comparator.notifyNewBottom(new BsonString("b"));

    assertTrue(comparator.hasValue(0)); // "b"
    assertThat(comparator.compareBottomToCurrent()).isGreaterThan(0);

    assertTrue(comparator.hasValue(1)); // "b"
    assertThat(comparator.compareBottomToCurrent()).isEqualTo(0);

    assertTrue(comparator.hasValue(2)); // "c"
    assertThat(comparator.compareBottomToCurrent()).isLessThan(0);
  }

  @Test
  public void compareBottomNoMatch() throws IOException {
    var comparator = createComparator(BsonUtils.MAX_KEY);
    comparator.notifyNewBottom(new BsonString("b_not_in_data"));

    assertTrue(comparator.hasValue(1)); // "b"
    assertThat(comparator.compareBottomToCurrent()).isGreaterThan(0);

    assertTrue(comparator.hasValue(2)); // "c"
    assertThat(comparator.compareBottomToCurrent()).isLessThan(0);
  }
}
