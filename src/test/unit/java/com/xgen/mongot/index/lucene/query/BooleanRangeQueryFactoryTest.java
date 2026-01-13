package com.xgen.mongot.index.lucene.query;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static org.mockito.Mockito.mock;

import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.field.FieldValue;
import com.xgen.mongot.index.lucene.query.BooleanRangeQueryFactory.EffectiveValue;
import com.xgen.mongot.index.lucene.query.context.SearchQueryFactoryContext;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.operators.bound.BooleanRangeBound;
import com.xgen.mongot.index.query.points.BooleanPoint;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import java.util.Arrays;
import java.util.Collection;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class BooleanRangeQueryFactoryTest {

  private static final BooleanPoint TRUE = new BooleanPoint(true);
  private static final BooleanPoint FALSE = new BooleanPoint(false);

  private final BooleanRangeBound rangeBound;
  private final EffectiveValue expected;

  public BooleanRangeQueryFactoryTest(BooleanRangeBound rangeBound, EffectiveValue expected) {
    this.rangeBound = rangeBound;
    this.expected = expected;
  }

  @Parameterized.Parameters(name = "{0}: {1}")
  public static Collection<Object[]> data() {
    return Arrays.asList(
        new Object[][] {
          {new BooleanRangeBound(of(FALSE), empty(), false, false), EffectiveValue.TRUE},
          {new BooleanRangeBound(of(FALSE), empty(), true, false), EffectiveValue.EITHER},
          {new BooleanRangeBound(of(TRUE), empty(), false, false), EffectiveValue.NEITHER},
          {new BooleanRangeBound(of(TRUE), empty(), true, false), EffectiveValue.TRUE},
          {new BooleanRangeBound(empty(), of(FALSE), false, false), EffectiveValue.NEITHER},
          {new BooleanRangeBound(empty(), of(FALSE), false, true), EffectiveValue.FALSE},
          {new BooleanRangeBound(empty(), of(TRUE), false, false), EffectiveValue.FALSE},
          {new BooleanRangeBound(empty(), of(TRUE), false, true), EffectiveValue.EITHER},
          {new BooleanRangeBound(of(FALSE), of(TRUE), true, true), EffectiveValue.EITHER},
          {new BooleanRangeBound(of(FALSE), of(TRUE), true, false), EffectiveValue.FALSE},
          {new BooleanRangeBound(of(FALSE), of(TRUE), false, false), EffectiveValue.NEITHER},
          {new BooleanRangeBound(of(FALSE), of(TRUE), false, true), EffectiveValue.TRUE},
          {new BooleanRangeBound(of(TRUE), of(FALSE), true, true), EffectiveValue.NEITHER},
          {new BooleanRangeBound(empty(), empty(), true, true), EffectiveValue.EITHER},
        });
  }

  @Test
  public void shouldTestRangeQuery() throws InvalidQueryException {
    var fieldPath = FieldPath.newRoot("path");
    var query =
        new BooleanRangeQueryFactory(new EqualsQueryFactory(mock(SearchQueryFactoryContext.class)))
            .fromBounds(this.rangeBound)
            .apply(fieldPath, empty());

    Assert.assertEquals(this.expected, convertToEffectiveValue(query, fieldPath));
  }

  private EffectiveValue convertToEffectiveValue(Query query, FieldPath fieldPath) {

    if (query.equals(getEqualsQuery(true, fieldPath))) {
      return EffectiveValue.TRUE;
    }

    if (query.equals(getEqualsQuery(false, fieldPath))) {
      return EffectiveValue.FALSE;
    }

    if (query.equals(getBooleanQuery(fieldPath))) {
      return EffectiveValue.EITHER;
    }

    if (query instanceof MatchNoDocsQuery) {
      return EffectiveValue.NEITHER;
    }

    return Check.unreachable();
  }

  private Query getBooleanQuery(FieldPath fieldPath) {
    return new BooleanQuery.Builder()
        .add(getEqualsQuery(true, fieldPath), BooleanClause.Occur.SHOULD)
        .add(getEqualsQuery(false, fieldPath), BooleanClause.Occur.SHOULD)
        .build();
  }

  private Query getEqualsQuery(boolean value, FieldPath fieldPath) {
    var field = value ? FieldValue.BOOLEAN_TRUE_FIELD_VALUE : FieldValue.BOOLEAN_FALSE_FIELD_VALUE;
    return new ConstantScoreQuery(
        new TermQuery(
            new Term(FieldName.TypeField.BOOLEAN.getLuceneFieldName(fieldPath, empty()), field)));
  }
}
