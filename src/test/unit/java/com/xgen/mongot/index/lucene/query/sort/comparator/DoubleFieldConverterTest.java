package com.xgen.mongot.index.lucene.query.sort.comparator;

import static org.junit.Assert.assertEquals;

import com.xgen.mongot.index.lucene.util.LuceneDoubleConversionUtils;
import com.xgen.mongot.index.query.sort.NullEmptySortPosition;
import com.xgen.testing.TestUtils;
import org.bson.BsonValue;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

@RunWith(Theories.class)
public class DoubleFieldConverterTest {

  @DataPoints public static final double[] parameters = TestUtils.createWeirdDoubles();

  @DataPoints public static final long[] sentinels = {Long.MIN_VALUE, Long.MAX_VALUE};

  @DataPoints public static final NullEmptySortPosition[] noData = NullEmptySortPosition.values();

  @Theory
  public void encodeToBson_nonNull_isBijective(
      double input, long nullSentinel, NullEmptySortPosition noData) {
    DoubleFieldConverter converter = new DoubleFieldConverter(nullSentinel);

    long encoded = LuceneDoubleConversionUtils.toMqlSortableLong(input);

    BsonValue bson = converter.encodeToBson(encoded, noData.getNullMissingSortValue());
    long result = converter.decodeFromBson(bson, noData.getNullMissingSortValue());

    assertEquals(encoded, result);
  }

  @Theory
  public void encodeToBson_null_isBijective(long nullSentinel, NullEmptySortPosition noData) {
    DoubleFieldConverter converter = new DoubleFieldConverter(nullSentinel);

    BsonValue bson = converter.encodeToBson(nullSentinel, noData.getNullMissingSortValue());
    long result = converter.decodeFromBson(bson, noData.getNullMissingSortValue());

    assertEquals(bson, noData.getNullMissingSortValue());
    assertEquals(nullSentinel, result);
  }

  @Theory
  public void encodeToBson_missing_isBijective(NullEmptySortPosition noData) {
    DoubleFieldConverter converter = new DoubleFieldConverter(Long.MIN_VALUE);

    BsonValue bson = converter.encodeToBson(Long.MIN_VALUE, noData.getNullMissingSortValue());
    long result = converter.decodeFromBson(bson, noData.getNullMissingSortValue());

    assertEquals(bson, noData.getNullMissingSortValue());
    assertEquals(Long.MIN_VALUE, result);
  }
}
