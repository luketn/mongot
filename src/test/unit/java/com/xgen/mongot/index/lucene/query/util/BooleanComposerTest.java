package com.xgen.mongot.index.lucene.query.util;

import com.xgen.mongot.index.query.InvalidQueryException;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.junit.Assert;
import org.junit.Test;

public class BooleanComposerTest {
  @Test
  public void testMap() {
    var result =
        BooleanComposer.StreamUtils.from(List.of("a", "b"))
            .map(this::termQuery, BooleanClause.Occur.SHOULD);
    var expected = BooleanComposer.should(termQuery("a"), termQuery("b"));
    Assert.assertEquals("translated to", expected, result);
  }

  @Test
  public void testMapWithBoundSecondArgument() throws InvalidQueryException {
    var result =
        BooleanComposer.StreamUtils.from(List.of("a", "b"))
            .mapWithBoundSecondArgument(
                Optional.of("embeddedRoot"), this::termQuery, BooleanClause.Occur.SHOULD);
    var expected =
        BooleanComposer.should(
            termQuery("a", Optional.of("embeddedRoot")),
            termQuery("b", Optional.of("embeddedRoot")));
    Assert.assertEquals("translated to", expected, result);
  }

  @Test
  public void testMapChecked() throws InvalidQueryException {
    var result =
        BooleanComposer.StreamUtils.from(List.of("a", "b"))
            .mapChecked(this::termQueryChecked, BooleanClause.Occur.SHOULD);
    var expected = BooleanComposer.should(termQuery("a"), termQuery("b"));
    Assert.assertEquals("translated to", expected, result);
  }

  @Test
  public void testMapCheckedWithBoundSecondArgument() throws InvalidQueryException {
    var result =
        BooleanComposer.StreamUtils.from(List.of("a", "b"))
            .mapCheckedWithBoundSecondArgument(
                Optional.of("embeddedRoot"), this::termQueryChecked, BooleanClause.Occur.SHOULD);
    var expected =
        BooleanComposer.should(
            termQuery("a", Optional.of("embeddedRoot")),
            termQuery("b", Optional.of("embeddedRoot")));
    Assert.assertEquals("translated to", expected, result);
  }

  @Test
  public void testMapCheckedPropagatesException() {
    BooleanComposer.StreamUtils<String> disjunction =
        BooleanComposer.StreamUtils.from(List.of("a", "b"));
    Assert.assertThrows(
        InvalidQueryException.class,
        () ->
            disjunction.mapChecked(
                s -> {
                  throw new InvalidQueryException("boom");
                },
                BooleanClause.Occur.SHOULD));
  }

  @Test
  public void testMapCheckedWithBoundSecondArgumentPropagatesException()
      throws InvalidQueryException {
    BooleanComposer.StreamUtils<String> disjunction =
        BooleanComposer.StreamUtils.from(List.of("a", "b"));
    Assert.assertThrows(
        InvalidQueryException.class,
        () ->
            disjunction.mapCheckedWithBoundSecondArgument(
                Optional.empty(),
                (a, b) -> {
                  throw new InvalidQueryException("boom");
                },
                BooleanClause.Occur.SHOULD));
  }

  @Test
  public void testMapOptional() {
    var result =
        BooleanComposer.StreamUtils.from(List.of("a", "b"))
            .mapOptional(
                text -> {
                  if (text.equals("a")) {
                    return Optional.empty();
                  } else {
                    return Optional.of(termQuery(text));
                  }
                },
                BooleanClause.Occur.SHOULD);
    var expected = termQuery("b"); // one query does not need to be nested under boolean.should
    Assert.assertEquals("translated to", expected, result);
  }

  @Test
  public void testMapCheckedOptional() throws InvalidQueryException {
    var result =
        BooleanComposer.StreamUtils.from(List.of("a", "b"))
            .mapOptionalChecked(this::optionalTermQueryChecked, BooleanClause.Occur.SHOULD);
    var expected = termQuery("b"); // one query does not need to be nested under boolean.should
    Assert.assertEquals("translated to", expected, result);
  }

  @Test(expected = InvalidQueryException.class)
  public void testMapCheckedOptionalPropagatesException() throws InvalidQueryException {
    BooleanComposer.StreamUtils.from(List.of("a", "b", "c"))
        .mapOptionalChecked(this::optionalTermQueryChecked, BooleanClause.Occur.SHOULD);
  }

  private Query termQuery(String text) {
    return new TermQuery(new Term("f", text));
  }

  private Query termQuery(String path, Optional<String> embeddedRoot) {
    return new TermQuery(
        new Term(embeddedRoot.map(rootPath -> path + "." + rootPath).orElse(path)));
  }

  private Query termQueryChecked(String text) throws InvalidQueryException {
    return new TermQuery(new Term("f", text));
  }

  private Query termQueryChecked(String path, Optional<String> embeddedRoot)
      throws InvalidQueryException {
    return new TermQuery(
        new Term(embeddedRoot.map(rootPath -> path + "." + rootPath).orElse(path)));
  }

  private Optional<Query> optionalTermQueryChecked(String text) throws InvalidQueryException {
    if (text.equals("a")) {
      return Optional.empty();
    } else if (text.equals("b")) {
      return Optional.of(termQuery(text));
    } else {
      throw new InvalidQueryException("error");
    }
  }
}
