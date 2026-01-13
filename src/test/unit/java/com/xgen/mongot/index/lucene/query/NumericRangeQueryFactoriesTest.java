package com.xgen.mongot.index.lucene.query;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.mock;

import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.analyzer.AnalyzerRegistry;
import com.xgen.mongot.index.lucene.query.util.BooleanComposer;
import com.xgen.mongot.index.lucene.util.LuceneDoubleConversionUtils;
import com.xgen.mongot.index.query.QueryOptimizationFlags;
import com.xgen.mongot.index.query.operators.RangeOperator;
import com.xgen.mongot.index.query.points.DoublePoint;
import com.xgen.mongot.index.query.points.LongPoint;
import com.xgen.mongot.index.query.points.NumericPoint;
import com.xgen.mongot.index.synonym.SynonymRegistry;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.query.operators.OperatorBuilder;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class NumericRangeQueryFactoriesTest {
  private static final String PATH_QUANTITY = "quantity";

  private static Directory directory;
  private static IndexWriter writer;

  /** set up an index. */
  @Before
  public void setUp() throws IOException {
    TemporaryFolder temporaryFolder = TestUtils.getTempFolder();
    directory = new MMapDirectory(temporaryFolder.getRoot().toPath());
    writer = new IndexWriter(directory, new IndexWriterConfig());
    writer.commit();
  }

  @After
  public void tearDown() throws IOException {
    writer.close();
    directory.close();
  }

  private static Query expectedQueryForBounds(
      String path,
      NumericPoint lower,
      boolean lowerInclusive,
      NumericPoint upper,
      boolean upperInclusive) {
    return new ConstantScoreQuery(
        new BooleanQuery.Builder()
            .add(
                expectedLongQuery(path, lower, lowerInclusive, upper, upperInclusive),
                BooleanClause.Occur.SHOULD)
            .add(
                expectedDoubleQuery(path, lower, lowerInclusive, upper, upperInclusive),
                BooleanClause.Occur.SHOULD)
            .build());
  }

  private static Query expectedLongQuery(
      String path,
      NumericPoint lower,
      boolean lowerInclusive,
      NumericPoint upper,
      boolean upperInclusive) {
    long lowerBound =
        switch (lower) {
          case DoublePoint dp -> longBoundForDouble(dp.value(), lowerInclusive, false);
          case LongPoint lp -> longBoundForLong(lp.value(), lowerInclusive, false);
        };
    long upperBound =
        switch (upper) {
          case DoublePoint dp -> longBoundForDouble(dp.value(), upperInclusive, true);
          case LongPoint lp -> longBoundForLong(lp.value(), upperInclusive, true);
        };

    if (upperBound < lowerBound) {
      return new MatchNoDocsQuery(
          String.format(
              "%s bounds are outside representable range of long",
              String.format(
                  "%s%s, %s%s",
                  lowerInclusive ? "[" : "(", lower, upper, upperInclusive ? "]" : ")")));
    }

    return createLongLuceneRangeQueries(path, lowerBound, upperBound);
  }

  private static Query expectedDoubleQuery(
      String path,
      NumericPoint lower,
      boolean lowerInclusive,
      NumericPoint upper,
      boolean upperInclusive) {
    double lowerBound =
        switch (lower) {
          case DoublePoint dp -> doubleBound(dp.value(), lowerInclusive, false);
          case LongPoint lp -> doubleBound(lp.value(), lowerInclusive, false);
        };
    double upperBound =
        switch (upper) {
          case DoublePoint dp -> doubleBound(dp.value(), upperInclusive, true);
          case LongPoint lp -> doubleBound(lp.value(), upperInclusive, true);
        };

    if (upperBound < lowerBound) {
      return new MatchNoDocsQuery(
          String.format(
              "%s bounds are outside representable range of double",
              String.format(
                  "%s%s, %s%s",
                  lowerInclusive ? "[" : "(", lower, upper, upperInclusive ? "]" : ")")));
    }

    return createDoubleLuceneRangeQueries(path, lowerBound, upperBound);
  }

  private static long longBoundForLong(long value, boolean inclusive, boolean isUpper) {
    boolean isExclusive = !inclusive;
    boolean exclusiveWouldNotPushLowerOutOfBounds = isUpper || value < Long.MAX_VALUE;
    boolean exclusiveWouldNotPushUpperOutOfBounds = !isUpper || value > Long.MIN_VALUE;

    boolean shouldModifyScore =
        isExclusive
            && exclusiveWouldNotPushLowerOutOfBounds
            && exclusiveWouldNotPushUpperOutOfBounds;
    long exclusiveDelta = isUpper ? -1 : 1;

    return value + (shouldModifyScore ? exclusiveDelta : 0);
  }

  private static long longBoundForDouble(double value, boolean inclusive, boolean isUpper) {
    if (value < Long.MIN_VALUE) {
      return Long.MIN_VALUE;
    }

    if (value > Long.MAX_VALUE) {
      return Long.MAX_VALUE;
    }

    boolean isIntegral = value % 1 == 0;
    boolean isExclusive = !inclusive;
    boolean exclusiveWouldNotPushLowerOutOfBounds = (isUpper || value < Double.MAX_VALUE);
    boolean exclusiveWouldNotPushUpperOutOfBounds = (!isUpper || value > -1.0 * Double.MAX_VALUE);

    // This could be more efficient, but is much easier to read and understand this way.
    boolean shouldModifyBound =
        isIntegral
            && isExclusive
            && exclusiveWouldNotPushLowerOutOfBounds
            && exclusiveWouldNotPushUpperOutOfBounds;
    long exclusiveIntegralDelta = isUpper ? -1 : 1;
    long delta = shouldModifyBound ? exclusiveIntegralDelta : 0;

    // the number that will be the bound if the bound is either inclusive or fractional
    // [3.9, 10.1] -> [4, 10]
    // (3.9, 10.1) -> [4, 10]
    // [4.0, 10.0] -> [4, 10]
    // (4.0, 10.0) -> [5,  9]
    long truncatedNumber =
        Double.valueOf(isUpper ? Math.floor(value) : Math.ceil(value)).longValue();
    return truncatedNumber + delta;
  }

  private static double doubleBound(double value, boolean inclusive, boolean isUpper) {
    if (inclusive) {
      return value;
    }
    return isUpper ? Math.nextDown(value) : Math.nextUp(value);
  }

  static class NumericTestCase {
    public final String description;
    private final String path;
    private final NumericPoint lower;
    private final NumericPoint upper;
    private final boolean lowerInclusive;
    private final boolean upperInclusive;

    NumericTestCase(
        String description,
        String path,
        NumericPoint lower,
        NumericPoint upper,
        boolean lowerInclusive,
        boolean upperInclusive) {
      this.description = description;
      this.path = path;
      this.lower = lower;
      this.upper = upper;
      this.lowerInclusive = lowerInclusive;
      this.upperInclusive = upperInclusive;
    }

    static NumericTestCase fromBounds(
        NumericPoint lower, NumericPoint upper, boolean lowerInclusive, boolean upperInclusive) {
      return new NumericTestCase(
          "fromBounds: long long: ", "somePath", lower, upper, lowerInclusive, upperInclusive);
    }

    // Creates a set of variants that include this value or a value adjacent to it in a bound.
    static Collection<NumericTestCase> variantsNear(long value) {
      List<NumericTestCase> variants = new ArrayList<>();
      variants.addAll(openAt(value - 1L));
      variants.addAll(openAt(value));
      variants.addAll(openAt(value + 1L));

      variants.addAll(
          closedAt(new LongPoint(value), new LongPoint(value - 1L), new LongPoint(value + 1L)));
      return variants;
    }

    // Creates a set of variants that include this value or a value adjacent to it in a bound.
    static Collection<NumericTestCase> variantsNear(double value) {
      List<NumericTestCase> variants = new ArrayList<>();
      variants.addAll(openAt(Math.nextDown(value)));
      variants.addAll(openAt(value));
      variants.addAll(openAt(Math.nextUp(value)));
      variants.addAll(
          closedAt(
              new DoublePoint(value),
              new DoublePoint(Math.nextDown(value)),
              new DoublePoint(Math.nextUp(value))));
      return variants;
    }

    static Collection<NumericTestCase> openAt(long value) {
      return closedAt(
          new LongPoint(value), new LongPoint(Long.MIN_VALUE), new LongPoint(Long.MAX_VALUE));
    }

    static Collection<NumericTestCase> openAt(double value) {
      return closedAt(
          new DoublePoint(value),
          new DoublePoint(-1.0 * Double.MAX_VALUE),
          new DoublePoint(Double.MAX_VALUE));
    }

    // Adds these variants:
    // [min, value], [min, value)
    // [value, max], (value, max],
    static Collection<NumericTestCase> closedAt(
        NumericPoint value, NumericPoint min, NumericPoint max) {
      List<NumericTestCase> variants = new ArrayList<>();
      variants.add(fromBounds(value, max, true, true));
      variants.add(fromBounds(value, max, false, true));
      variants.add(fromBounds(min, value, true, true));
      variants.add(fromBounds(min, value, true, false));
      return variants;
    }

    @Override
    @SuppressWarnings("ConditionalExpressionNumericPromotion")
    public String toString() {
      return String.format(
          "%s: %s%s, %s%s",
          this.description,
          this.lowerInclusive ? "[" : "(",
          switch (this.lower) {
            case DoublePoint dp -> dp.value();
            case LongPoint lp -> lp.value();
          },
          switch (this.upper) {
            case DoublePoint dp -> dp.value();
            case LongPoint lp -> lp.value();
          },
          this.upperInclusive ? "]" : ")");
    }

    void test() {
      try {
        RangeOperator definition =
            OperatorBuilder.range()
                .path(this.path)
                .numericBounds(
                    Optional.of(this.lower),
                    Optional.of(this.upper),
                    this.lowerInclusive,
                    this.upperInclusive)
                .build();

        Query expected =
            expectedQueryForBounds(
                this.path, this.lower, this.lowerInclusive, this.upper, this.upperInclusive);

        LuceneSearchQueryFactoryDistributor factory =
            NumericRangeQueryFactoriesTest.createFactory();
        Query result =
            factory.createQuery(
                definition,
                DirectoryReader.open(directory),
                QueryOptimizationFlags.DEFAULT_OPTIONS);
        Assert.assertEquals("long double initiated must match:", expected, result);
      } catch (Throwable e) {
        Assert.fail(
            String.format("numeric test case failed: %s: %s", this.description, e.getMessage()));
      }
    }
  }

  @Test
  public void testZero() throws Exception {
    NumericTestCase.variantsNear(0L).forEach(NumericTestCase::test);
    NumericTestCase.variantsNear(0.0).forEach(NumericTestCase::test);
  }

  @Test
  public void testNegOne() throws Exception {
    NumericTestCase.variantsNear(-1L).forEach(NumericTestCase::test);
    NumericTestCase.variantsNear(-1.0).forEach(NumericTestCase::test);
  }

  @Test
  public void testOne() throws Exception {
    NumericTestCase.variantsNear(1L).forEach(NumericTestCase::test);
    NumericTestCase.variantsNear(1.0).forEach(NumericTestCase::test);
  }

  @Test
  public void testMinLong() throws Exception {
    List.of(
            NumericTestCase.fromBounds(
                new LongPoint(Long.MIN_VALUE), new LongPoint(Long.MIN_VALUE), true, true),
            NumericTestCase.fromBounds(
                new LongPoint(Long.MIN_VALUE), new LongPoint(Long.MIN_VALUE + 1), true, false),
            NumericTestCase.fromBounds(
                new DoublePoint(2.0 * Long.MIN_VALUE), new LongPoint(Long.MIN_VALUE), true, true))
        .forEach(NumericTestCase::test);
  }

  @Test
  public void testMaxLong() throws Exception {
    List.of(
            NumericTestCase.fromBounds(
                new LongPoint(Long.MAX_VALUE), new LongPoint(Long.MAX_VALUE), true, true),
            NumericTestCase.fromBounds(
                new LongPoint(Long.MAX_VALUE - 1L), new LongPoint(Long.MAX_VALUE), false, true),
            NumericTestCase.fromBounds(
                new LongPoint(Long.MAX_VALUE),
                new DoublePoint(Math.nextUp(Double.valueOf(Long.MAX_VALUE))),
                true,
                true),
            NumericTestCase.fromBounds(
                new LongPoint(Long.MAX_VALUE),
                new DoublePoint(Math.nextUp(Double.valueOf(Long.MAX_VALUE))),
                true,
                false))
        .forEach(NumericTestCase::test);
  }

  @Test
  public void testNumericRangeQueryUpperInclusive() throws Exception {
    RangeOperator definition =
        OperatorBuilder.range()
            .path(PATH_QUANTITY)
            .numericBounds(Optional.empty(), Optional.of(new LongPoint(1L)), false, true)
            .build();

    Query expected =
        new ConstantScoreQuery(
            new BooleanQuery.Builder()
                .add(createLongLuceneRangeQueries("quantity", Long.MIN_VALUE, 1L), Occur.SHOULD)
                .add(
                    createDoubleLuceneRangeQueries("quantity", -1.0 * Double.MAX_VALUE, 1.0),
                    Occur.SHOULD)
                .build());

    LuceneSearchQueryFactoryDistributor factory = createFactory();
    Query result =
        factory.createQuery(
            definition, DirectoryReader.open(directory), QueryOptimizationFlags.DEFAULT_OPTIONS);
    Assert.assertEquals("numeric range upper inclusive", expected, result);
  }

  @Test
  public void testNumericRangeQueryUpperExclusive() throws Exception {
    RangeOperator definition =
        OperatorBuilder.range()
            .path(PATH_QUANTITY)
            .numericBounds(Optional.empty(), Optional.of(new LongPoint(1L)), true, false)
            .build();

    Query expected =
        new ConstantScoreQuery(
            new BooleanQuery.Builder()
                .add(createLongLuceneRangeQueries("quantity", Long.MIN_VALUE, 0L), Occur.SHOULD)
                .add(
                    createDoubleLuceneRangeQueries(
                        "quantity", -1.0 * Double.MAX_VALUE, Math.nextDown(1.0)),
                    Occur.SHOULD)
                .build());

    LuceneSearchQueryFactoryDistributor factory = createFactory();
    Query result =
        factory.createQuery(
            definition, DirectoryReader.open(directory), QueryOptimizationFlags.DEFAULT_OPTIONS);
    Assert.assertEquals("numeric range upper exclusive", expected, result);
  }

  @Test
  public void testNumericRangeQueryLowerExclusive() throws Exception {
    RangeOperator definition =
        OperatorBuilder.range()
            .path(PATH_QUANTITY)
            .numericBounds(Optional.of(new LongPoint(2345L)), Optional.empty(), false, true)
            .build();

    Query expected =
        new ConstantScoreQuery(
            new BooleanQuery.Builder()
                .add(createLongLuceneRangeQueries("quantity", 2346L, Long.MAX_VALUE), Occur.SHOULD)
                .add(
                    createDoubleLuceneRangeQueries(
                        "quantity", 2345.0 + Math.ulp(2345.0), Double.MAX_VALUE),
                    Occur.SHOULD)
                .build());

    LuceneSearchQueryFactoryDistributor factory = createFactory();
    Query result =
        factory.createQuery(
            definition, DirectoryReader.open(directory), QueryOptimizationFlags.DEFAULT_OPTIONS);
    Assert.assertEquals("numeric range lower exclusive", expected, result);
  }

  /**
   * Inclusive double bounds without fractional components should translate to correct integral and
   * floating point bounds.
   *
   * <p>{@code [2.0, 9.0]} should turn into either:
   *
   * <ul>
   *   <li>{@code [2.0, 9.0]} over doubles
   *   <li>{@code [2, 9]} over integers
   * </ul>
   */
  @Test
  public void testNumericRangeQueryDouble() throws Exception {
    String path = "quantity";
    NumericPoint lower = new DoublePoint(2.0);
    boolean lowerInclusive = true;
    NumericPoint upper = new DoublePoint(9.0);
    boolean upperInclusive = true;

    RangeOperator definition =
        OperatorBuilder.range()
            .path(path)
            .numericBounds(Optional.of(lower), Optional.of(upper), lowerInclusive, upperInclusive)
            .build();

    Query expected = expectedQueryForBounds(path, lower, lowerInclusive, upper, upperInclusive);

    LuceneSearchQueryFactoryDistributor factory = createFactory();
    Query result =
        factory.createQuery(
            definition, DirectoryReader.open(directory), QueryOptimizationFlags.DEFAULT_OPTIONS);
    Assert.assertEquals("numeric range double inclusive", expected, result);
  }

  /**
   * Double bounds with fractional components should translate to correct integral and floating
   * point bounds.
   *
   * <p>{@code (3.11, 9.22)} should turn into the union of two ranges:
   *
   * <ul>
   *   <li>{@code [3.11 + e, 9.22 - e]} over doubles
   *   <li>{@code [4, 9]} over integers
   * </ul>
   */
  @Test
  public void testNumericRangeQueryDoubleBothExclusiveBothFractional() throws Exception {
    RangeOperator definition =
        OperatorBuilder.range()
            .path(PATH_QUANTITY)
            .numericBounds(
                Optional.of(new DoublePoint(3.11)),
                Optional.of(new DoublePoint(9.22)),
                false,
                false)
            .build();

    Query expected =
        new ConstantScoreQuery(
            new BooleanQuery.Builder()
                .add(createLongLuceneRangeQueries("quantity", 4L, 9L), BooleanClause.Occur.SHOULD)
                .add(
                    createDoubleLuceneRangeQueries(
                        "quantity", 3.11 + Math.ulp(3.11), 9.22 - Math.ulp(9.22)),
                    BooleanClause.Occur.SHOULD)
                .build());

    LuceneSearchQueryFactoryDistributor factory = createFactory();
    Query result =
        factory.createQuery(
            definition, DirectoryReader.open(directory), QueryOptimizationFlags.DEFAULT_OPTIONS);
    Assert.assertEquals("numeric range double fractional exclusive", expected, result);
  }

  @Test
  public void testNaN() throws Exception {
    // TODO(CLOUDP-213403): fix range queries for non-finite values.
    RangeOperator definition =
        OperatorBuilder.range()
            .path(PATH_QUANTITY)
            .numericBounds(
                Optional.of(new DoublePoint(Double.NaN)),
                Optional.of(new DoublePoint(Double.NaN)),
                true,
                true)
            .build();

    Query expected =
        new ConstantScoreQuery(
            new BooleanQuery.Builder()
                .add(createLongLuceneRangeQueries("quantity", 0L, 0L), BooleanClause.Occur.SHOULD)
                .add(
                    new MatchNoDocsQuery(
                        "[NaN, NaN] bounds are outside representable range of double"),
                    BooleanClause.Occur.SHOULD)
                .build());

    LuceneSearchQueryFactoryDistributor factory = createFactory();
    Query result =
        factory.createQuery(
            definition, DirectoryReader.open(directory), QueryOptimizationFlags.DEFAULT_OPTIONS);
    Assert.assertEquals("numeric range nan", expected, result);
  }

  @Test
  public void testNaNExclusive() {
    IllegalArgumentException e =
        Assert.assertThrows(
            IllegalArgumentException.class,
            () ->
                OperatorBuilder.range()
                    .path(PATH_QUANTITY)
                    .numericBounds(
                        Optional.of(new DoublePoint(Double.NaN)),
                        Optional.of(new DoublePoint(Double.NaN)),
                        false,
                        false)
                    .build());

    assertThat(e)
        .hasCauseThat()
        .hasMessageThat()
        .contains("bounds must both be inclusive if they are equal");
  }

  @Test
  public void testNumericRangeQueryLowerDoubleUpperNaN() throws Exception {
    RangeOperator definition =
        OperatorBuilder.range()
            .path(PATH_QUANTITY)
            .numericBounds(
                Optional.of(new DoublePoint(2.0)),
                Optional.of(new DoublePoint(Double.NaN)),
                false,
                true)
            .build();

    Query expected =
        expectedQueryForBounds(
            PATH_QUANTITY, new DoublePoint(2.0), false, new DoublePoint(Double.NaN), true);

    LuceneSearchQueryFactoryDistributor factory = createFactory();
    Query result =
        factory.createQuery(
            definition, DirectoryReader.open(directory), QueryOptimizationFlags.DEFAULT_OPTIONS);
    Assert.assertEquals("numeric range lower double upper nan", expected, result);
  }

  @Test
  public void testNumericRangeQueryLowerLongUpperNaN() throws Exception {
    RangeOperator definition =
        OperatorBuilder.range()
            .path(PATH_QUANTITY)
            .numericBounds(
                Optional.of(new LongPoint(2L)),
                Optional.of(new DoublePoint(Double.NaN)),
                false,
                true)
            .build();

    Query expected =
        expectedQueryForBounds(
            PATH_QUANTITY, new LongPoint(2L), false, new DoublePoint(Double.NaN), true);

    LuceneSearchQueryFactoryDistributor factory = createFactory();
    Query result =
        factory.createQuery(
            definition, DirectoryReader.open(directory), QueryOptimizationFlags.DEFAULT_OPTIONS);
    Assert.assertEquals("numeric range lower long upper nan", expected, result);
  }

  @Test
  public void testNumericRangeQueryLowerNaNUpperDouble() {
    IllegalArgumentException e =
        Assert.assertThrows(
            IllegalArgumentException.class,
            () ->
                OperatorBuilder.range()
                    .path(PATH_QUANTITY)
                    .numericBounds(
                        Optional.of(new DoublePoint(Double.NaN)),
                        Optional.of(new DoublePoint(2.0)),
                        true,
                        false)
                    .build());

    assertThat(e)
        .hasCauseThat()
        .hasMessageThat()
        .contains("gt/gte must not be greater than lt/lte");
  }

  @Test
  public void testNumericRangeQueryLowerNaNUpperLong() throws Exception {
    RangeOperator definition =
        OperatorBuilder.range()
            .path(PATH_QUANTITY)
            .numericBounds(
                Optional.of(new DoublePoint(Double.NaN)),
                Optional.of(new LongPoint(1L)),
                true,
                false)
            .build();

    Query expected =
        new ConstantScoreQuery(
            new BooleanQuery.Builder()
                .add(createLongLuceneRangeQueries("quantity", 0L, 0L), Occur.SHOULD)
                .add(
                    new MatchNoDocsQuery(
                        "[NaN, 1) bounds are outside representable range of double"),
                    Occur.SHOULD)
                .build());

    LuceneSearchQueryFactoryDistributor factory = createFactory();
    Query result =
        factory.createQuery(
            definition, DirectoryReader.open(directory), QueryOptimizationFlags.DEFAULT_OPTIONS);
    Assert.assertEquals("numeric range lower nan upper long", expected, result);
  }

  @Test
  public void testEqualExclusiveBoundsDouble() {
    IllegalArgumentException e =
        Assert.assertThrows(
            IllegalArgumentException.class,
            () ->
                OperatorBuilder.range()
                    .path(PATH_QUANTITY)
                    .numericBounds(
                        Optional.of(new DoublePoint(13.0)),
                        Optional.of(new DoublePoint(13.0)),
                        false,
                        false)
                    .build());

    assertThat(e)
        .hasCauseThat()
        .hasMessageThat()
        .contains("bounds must both be inclusive if they are equal");
  }

  @Test
  public void testEqualExclusiveBoundsLong() {
    IllegalArgumentException e =
        Assert.assertThrows(
            IllegalArgumentException.class,
            () ->
                OperatorBuilder.range()
                    .path(PATH_QUANTITY)
                    .numericBounds(
                        Optional.of(new LongPoint(13L)),
                        Optional.of(new LongPoint(13L)),
                        false,
                        false)
                    .build());

    assertThat(e)
        .hasCauseThat()
        .hasMessageThat()
        .contains("bounds must both be inclusive if they are equal");
  }

  @Test
  public void testDifferentBoundTypes() throws Exception {
    RangeOperator definition =
        OperatorBuilder.range()
            .path(PATH_QUANTITY)
            .numericBounds(
                Optional.of(new LongPoint(2L)),
                Optional.of(new DoublePoint(Double.NaN)),
                false,
                true)
            .build();
    Query expected =
        expectedQueryForBounds(
            PATH_QUANTITY, new LongPoint(2L), false, new DoublePoint(Double.NaN), true);

    LuceneSearchQueryFactoryDistributor factory = createFactory();
    Query result =
        factory.createQuery(
            definition, DirectoryReader.open(directory), QueryOptimizationFlags.DEFAULT_OPTIONS);
    Assert.assertEquals("numeric range diff bound types", expected, result);
  }

  @Test
  public void testInfinity() throws Exception {
    // TODO(CLOUDP-213403): fix range queries for non-finite values.
    RangeOperator definition =
        OperatorBuilder.range()
            .path(PATH_QUANTITY)
            .numericBounds(
                Optional.of(new DoublePoint(Double.POSITIVE_INFINITY)),
                Optional.of(new DoublePoint(Double.POSITIVE_INFINITY)),
                true,
                true)
            .build();

    Query expected =
        new ConstantScoreQuery(
            new BooleanQuery.Builder()
                .add(
                    new MatchNoDocsQuery(
                        "[Infinity, Infinity] bounds are outside representable range of long"),
                    Occur.SHOULD)
                .add(
                    new MatchNoDocsQuery(
                        "[Infinity, Infinity] bounds are outside representable range of double"),
                    Occur.SHOULD)
                .build());

    LuceneSearchQueryFactoryDistributor factory = createFactory();
    Query result =
        factory.createQuery(
            definition, DirectoryReader.open(directory), QueryOptimizationFlags.DEFAULT_OPTIONS);
    Assert.assertEquals("numeric range infinity", expected, result);
  }

  @Test
  public void testNegativeInfinity() throws Exception {
    // TODO(CLOUDP-213403): fix range queries for non-finite values.
    RangeOperator definition =
        OperatorBuilder.range()
            .path(PATH_QUANTITY)
            .numericBounds(
                Optional.of(new DoublePoint(Double.NEGATIVE_INFINITY)),
                Optional.of(new DoublePoint(Double.NEGATIVE_INFINITY)),
                true,
                true)
            .build();

    Query expected =
        new ConstantScoreQuery(
            new BooleanQuery.Builder()
                .add(
                    new MatchNoDocsQuery(
                        "[-Infinity, -Infinity] bounds are outside representable range of long"),
                    BooleanClause.Occur.SHOULD)
                .add(
                    new MatchNoDocsQuery(
                        "[-Infinity, -Infinity] bounds are outside representable range of double"),
                    BooleanClause.Occur.SHOULD)
                .build());

    LuceneSearchQueryFactoryDistributor factory = createFactory();
    Query result =
        factory.createQuery(
            definition, DirectoryReader.open(directory), QueryOptimizationFlags.DEFAULT_OPTIONS);
    Assert.assertEquals("numeric range negative infinity", expected, result);
  }

  /**
   * Exclusive double bounds without fractional components should translate to correct integral and
   * floating point bounds.
   *
   * <p>{@code (2.0, 9.0)} should turn into either:
   *
   * <ul>
   *   <li>{@code [2.0 + e, 9.0 - e]} over doubles
   *   <li>{@code [3, 8]} over integers
   * </ul>
   */
  @Test
  public void testNumericRangeQueryDoubleBothExclusiveBothIntegral() throws Exception {
    RangeOperator definition =
        OperatorBuilder.range()
            .path(PATH_QUANTITY)
            .numericBounds(
                Optional.of(new DoublePoint(2.0)), Optional.of(new DoublePoint(9.0)), false, false)
            .build();

    Query expected =
        new ConstantScoreQuery(
            new BooleanQuery.Builder()
                .add(createLongLuceneRangeQueries("quantity", 3L, 8L), Occur.SHOULD)
                .add(
                    createDoubleLuceneRangeQueries(
                        "quantity", Math.nextUp(2.0), Math.nextDown(9.0)),
                    Occur.SHOULD)
                .build());

    LuceneSearchQueryFactoryDistributor factory = createFactory();
    Query result =
        factory.createQuery(
            definition, DirectoryReader.open(directory), QueryOptimizationFlags.DEFAULT_OPTIONS);
    Assert.assertEquals("numeric range double integral exclusive", expected, result);
  }

  /**
   * Inclusive double bounds with fractional components should translate to correct integral and
   * floating point bounds.
   *
   * <p>{@code [3.11, 9.22]} should turn into either:
   *
   * <ul>
   *   <li>{@code [3.11, 9.22]} over doubles
   *   <li>{@code [4, 9]} over integers
   * </ul>
   */
  @Test
  public void testNumericRangeQueryDoubleBothInclusiveBothFractional() throws Exception {
    RangeOperator definition =
        OperatorBuilder.range()
            .path(PATH_QUANTITY)
            .numericBounds(
                Optional.of(new DoublePoint(3.11)), Optional.of(new DoublePoint(9.22)), true, true)
            .build();

    Query expected =
        new ConstantScoreQuery(
            new BooleanQuery.Builder()
                .add(createLongLuceneRangeQueries("quantity", 4L, 9L), Occur.SHOULD)
                .add(createDoubleLuceneRangeQueries("quantity", 3.11, 9.22), Occur.SHOULD)
                .build());

    LuceneSearchQueryFactoryDistributor factory = createFactory();
    Query result =
        factory.createQuery(
            definition, DirectoryReader.open(directory), QueryOptimizationFlags.DEFAULT_OPTIONS);
    Assert.assertEquals("numeric range double fractional inclusive", expected, result);
  }

  /**
   * Inclusive double upper bound that is less than the min representable long value should
   * translate to a MatchNoDocsQuery for $type:long.
   *
   * <p>{@code [any, -1.0*MIN_LONG - e]} should turn into either:
   *
   * <ul>
   *   <li>{@code [any, -1.0*MIN_LONG - e]} over doubles
   *   <li>{@code ∅} over integers
   * </ul>
   */
  @Test
  public void testNumericRangeQueryLessThanMinLong() throws Exception {
    RangeOperator definition =
        OperatorBuilder.range()
            .path(PATH_QUANTITY)
            .numericBounds(
                Optional.of(new DoublePoint(-2.0E23)),
                Optional.of(new DoublePoint(Math.nextDown(Double.valueOf(Long.MIN_VALUE)))),
                true,
                true)
            .build();

    Query expected =
        new ConstantScoreQuery(
            new BooleanQuery.Builder()
                .add(
                    new MatchNoDocsQuery(
                        "[-2.0E23, -9.223372036854778E18] "
                            + "bounds are outside representable range of long"),
                    Occur.SHOULD)
                .add(
                    createDoubleLuceneRangeQueries(
                        "quantity", -2.0E23, Math.nextDown(Double.valueOf(Long.MIN_VALUE))),
                    Occur.SHOULD)
                .build());

    LuceneSearchQueryFactoryDistributor factory = createFactory();
    Query result =
        factory.createQuery(
            definition, DirectoryReader.open(directory), QueryOptimizationFlags.DEFAULT_OPTIONS);
    Assert.assertEquals("numeric range long less than min inclusive", expected, result);
  }

  /**
   * Exclusive double upper bound that is less than or equal to the min representable long value
   * should translate to a MatchNoDocsQuery for $type:long.
   *
   * <p>{@code [any, -1.0*MIN_LONG)} should turn into either:
   *
   * <ul>
   *   <li>{@code [any, -1.0*MIN_LONG - e]} over doubles
   *   <li>{@code ∅} over integers
   * </ul>
   */
  @Test
  public void testNumericRangeQueryEqualExclusiveToMinLong() throws Exception {
    RangeOperator definition =
        OperatorBuilder.range()
            .path(PATH_QUANTITY)
            .numericBounds(
                Optional.of(new DoublePoint(-2.0E23)),
                Optional.of(new DoublePoint(Double.valueOf(Long.MIN_VALUE))),
                true,
                false)
            .build();

    Query expected =
        new ConstantScoreQuery(
            new BooleanQuery.Builder()
                .add(
                    new MatchNoDocsQuery(
                        "[-2.0E23, -9.223372036854776E18) "
                            + "bounds are outside representable range of long"),
                    Occur.SHOULD)
                .add(
                    createDoubleLuceneRangeQueries(
                        "quantity", -2.0E23, Math.nextDown(Double.valueOf(Long.MIN_VALUE))),
                    Occur.SHOULD)
                .build());

    LuceneSearchQueryFactoryDistributor factory = createFactory();
    Query result =
        factory.createQuery(
            definition, DirectoryReader.open(directory), QueryOptimizationFlags.DEFAULT_OPTIONS);
    Assert.assertEquals("numeric range long equal to min exclusive", expected, result);
  }

  /**
   * Inclusive double lower bound that is greater than the min representable long value should
   * translate to a MatchNoDocsQuery for $type:long.
   *
   * <p>{@code [MAX_LONG + e, any]} should turn into either:
   *
   * <ul>
   *   <li>{@code [MAX_LONG + e, any]} over doubles
   *   <li>{@code ∅} over integers
   * </ul>
   */
  @Test
  public void testNumericRangeQueryGreaterThanMaxLong() throws Exception {
    RangeOperator definition =
        OperatorBuilder.range()
            .path(PATH_QUANTITY)
            .numericBounds(
                Optional.of(new DoublePoint(Math.nextUp(Double.valueOf(Long.MAX_VALUE)))),
                Optional.of(new DoublePoint(6.02E23)),
                true,
                true)
            .build();

    Query expected =
        new ConstantScoreQuery(
            new BooleanQuery.Builder()
                .add(
                    new MatchNoDocsQuery(
                        "[9.223372036854778E18, 6.02E23] bounds are "
                            + "outside representable range of long"),
                    Occur.SHOULD)
                .add(
                    createDoubleLuceneRangeQueries(
                        "quantity", Math.nextUp(Double.valueOf(Long.MAX_VALUE)), 6.02E23),
                    Occur.SHOULD)
                .build());

    LuceneSearchQueryFactoryDistributor factory = createFactory();
    Query result =
        factory.createQuery(
            definition, DirectoryReader.open(directory), QueryOptimizationFlags.DEFAULT_OPTIONS);
    Assert.assertEquals("numeric range long greater than max inclusive", expected, result);
  }

  /**
   * Exclusive double lower bound that is greater than or equal to the min representable long value
   * should translate to a MatchNoDocsQuery for $type:long.
   *
   * <p>{@code (MAX_LONG, any)} should turn into either:
   *
   * <ul>
   *   <li>{@code [MAX_LONG + e, any]} over doubles
   *   <li>{@code ∅} over integers
   * </ul>
   */
  @Test
  public void testNumericRangeQueryEqualExclusiveToMaxLong() throws Exception {
    RangeOperator definition =
        OperatorBuilder.range()
            .path(PATH_QUANTITY)
            .numericBounds(
                Optional.of(new DoublePoint(Double.valueOf(Long.MAX_VALUE))),
                Optional.of(new DoublePoint(6.02E23)),
                false,
                true)
            .build();

    Query expected =
        new ConstantScoreQuery(
            new BooleanQuery.Builder()
                .add(
                    new MatchNoDocsQuery(
                        "(9.223372036854776E18, 6.02E23] bounds "
                            + "are outside representable range of long"),
                    Occur.SHOULD)
                .add(
                    createDoubleLuceneRangeQueries(
                        "quantity", Math.nextUp(Double.valueOf(Long.MAX_VALUE)), 6.02E23),
                    Occur.SHOULD)
                .build());

    LuceneSearchQueryFactoryDistributor factory = createFactory();
    Query result =
        factory.createQuery(
            definition, DirectoryReader.open(directory), QueryOptimizationFlags.DEFAULT_OPTIONS);
    Assert.assertEquals("numeric range long equal to max exclusive", expected, result);
  }

  static LuceneSearchQueryFactoryDistributor createFactory() {
    return LuceneSearchQueryFactoryDistributor.create(
        SearchIndexDefinitionBuilder.VALID_INDEX,
        IndexFormatVersion.CURRENT,
        mock(AnalyzerRegistry.class),
        mock(SynonymRegistry.class),
        new IndexMetricsUpdater.QueryingMetricsUpdater(SearchIndex.mockMetricsFactory()),
        false,
        FeatureFlags.getDefault());
  }

  private static Query createLongLuceneRangeQueries(
      String fieldName, long lowerBound, long upperBound) {
    return BooleanComposer.constantScoreDisjunction(
        new IndexOrDocValuesQuery(
            org.apache.lucene.document.LongPoint.newRangeQuery(
                "$type:int64/" + fieldName, lowerBound, upperBound),
            NumericDocValuesField.newSlowRangeQuery(
                "$type:int64/" + fieldName, lowerBound, upperBound)),
        org.apache.lucene.document.LongPoint.newRangeQuery(
            "$type:int64Multiple/" + fieldName, lowerBound, upperBound));
  }

  private static Query createDoubleLuceneRangeQueries(
      String fieldName, double lowerBound, double upperBound) {
    long convertedLowerBound = LuceneDoubleConversionUtils.toLong(lowerBound);
    long convertedUpperBound = LuceneDoubleConversionUtils.toLong(upperBound);
    return BooleanComposer.constantScoreDisjunction(
        new IndexOrDocValuesQuery(
            org.apache.lucene.document.LongPoint.newRangeQuery(
                "$type:double/" + fieldName, convertedLowerBound, convertedUpperBound),
            NumericDocValuesField.newSlowRangeQuery(
                "$type:double/" + fieldName, convertedLowerBound, convertedUpperBound)),
        org.apache.lucene.document.LongPoint.newRangeQuery(
            "$type:doubleMultiple/" + fieldName, convertedLowerBound, convertedUpperBound));
  }
}
