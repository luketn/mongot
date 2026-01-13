package com.xgen.mongot.index.query.operators;

import com.xgen.mongot.index.query.scores.EmbeddedScore;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.index.query.operators.OperatorBuilder;
import com.xgen.testing.mongot.index.query.scores.ScoreBuilder;
import com.xgen.testing.mongot.index.query.scores.expressions.MultiplyExpressionBuilder;
import com.xgen.testing.mongot.index.query.scores.expressions.PathExpressionBuilder;
import com.xgen.testing.mongot.index.query.scores.expressions.ScoreExpressionBuilder;
import java.util.Optional;
import org.junit.Test;

public class OperatorEmbeddedRootValidatorTest {

  @Test
  public void testSimpleWithoutReturnScope() throws Exception {
    Operator textOperator = OperatorBuilder.text().path("path").query("query").build();
    OperatorEmbeddedRootValidator validator =
        new OperatorEmbeddedRootValidator(BsonParseContext.root());
    validator.validate(textOperator, Optional.empty());
  }

  @Test
  public void testSimpleWithReturnScope() throws Exception {
    Operator textOperator = OperatorBuilder.text().path("a.b").query("query").build();
    OperatorEmbeddedRootValidator validator =
        new OperatorEmbeddedRootValidator(BsonParseContext.root());
    validator.validate(textOperator, Optional.of(FieldPath.parse("a")));
  }

  @Test
  public void testSimpleWithInvalidReturnScope() {
    Operator textOperator = OperatorBuilder.text().path("path").query("query").build();
    OperatorEmbeddedRootValidator validator =
        new OperatorEmbeddedRootValidator(BsonParseContext.root());
    TestUtils.assertThrows(
        "text operator path 'path' must be a descendant of returnScope.path 'a.b.c'",
        BsonParseException.class,
        () -> validator.validate(textOperator, Optional.of(FieldPath.parse("a.b.c"))));
  }

  @Test
  public void testSimpleEmbeddedWithReturnScope() throws Exception {
    Operator embeddedOperator =
        OperatorBuilder.embeddedDocument()
            .path("a.b")
            .operator(OperatorBuilder.text().path("a.b.c").query("query").build())
            .build();
    OperatorEmbeddedRootValidator validator =
        new OperatorEmbeddedRootValidator(BsonParseContext.root());
    validator.validate(embeddedOperator, Optional.of(FieldPath.parse("a")));
  }

  @Test
  public void testSimpleEmbeddedWithoutReturnScope() throws Exception {
    Operator embeddedOperator =
        OperatorBuilder.embeddedDocument()
            .path("a.b")
            .operator(OperatorBuilder.text().path("a.b.c").query("query").build())
            .build();
    OperatorEmbeddedRootValidator validator =
        new OperatorEmbeddedRootValidator(BsonParseContext.root());
    validator.validate(embeddedOperator, Optional.empty());
  }

  @Test
  public void testSimpleEmbeddedWithInvalidReturnScope() {
    Operator embeddedOperator =
        OperatorBuilder.embeddedDocument()
            .path("a.b")
            .operator(OperatorBuilder.text().path("a.b.c").query("query").build())
            .build();
    OperatorEmbeddedRootValidator validator =
        new OperatorEmbeddedRootValidator(BsonParseContext.root());
    TestUtils.assertThrows(
        "embeddedDocument operator path 'a.b' must be a descendant of returnScope.path 'a.b.c.d'",
        BsonParseException.class,
        () -> validator.validate(embeddedOperator, Optional.of(FieldPath.parse("a.b.c.d"))));
  }

  @Test
  public void testNestedEmbeddedOperatorWithScoringValid() throws Exception {
    Operator embeddedOperator =
        OperatorBuilder.embeddedDocument()
            .path("a")
            .operator(
                OperatorBuilder.embeddedDocument()
                    .path("a.b")
                    .operator(
                        OperatorBuilder.exists()
                            .path("a.b.c")
                            .score(
                                ScoreBuilder.function()
                                    .expression(
                                        PathExpressionBuilder.builder().value("a.b.c").build())
                                    .build())
                            .build())
                    .score(
                        ScoreBuilder.embedded()
                            .aggregate(EmbeddedScore.Aggregate.MEAN)
                            .outerScore(
                                ScoreBuilder.function()
                                    .expression(
                                        MultiplyExpressionBuilder.builder()
                                            .arg(
                                                PathExpressionBuilder.builder()
                                                    .value("a.d")
                                                    .build())
                                            .arg(ScoreExpressionBuilder.builder().build())
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();
    OperatorEmbeddedRootValidator validator =
        new OperatorEmbeddedRootValidator(BsonParseContext.root());
    validator.validate(embeddedOperator, Optional.empty());
  }

  @Test
  public void testHasAncestorOperatorSimple() throws Exception {
    Operator hasAncestorOperator =
        OperatorBuilder.hasAncestor()
            .ancestorPath("a.b")
            .operator(OperatorBuilder.text().path("a.b.c").query("query").build())
            .build();
    OperatorEmbeddedRootValidator validator =
        new OperatorEmbeddedRootValidator(BsonParseContext.root());
    validator.validate(hasAncestorOperator, Optional.of(FieldPath.parse("a.b.c")));
  }

  @Test
  public void testHasAncestorWithValidIndirectParentOfAncestorPath() {
    Operator hasAncestorOperator =
        OperatorBuilder.hasAncestor()
            .ancestorPath("a.b")
            .operator(OperatorBuilder.text().path("a.c").query("query").build())
            .build();
    OperatorEmbeddedRootValidator validator =
        new OperatorEmbeddedRootValidator(BsonParseContext.root());
    TestUtils.assertThrows(
        "text operator path 'a.c' must be a descendant of hasAncestor.ancestorPath 'a.b'",
        BsonParseException.class,
        () -> validator.validate(hasAncestorOperator, Optional.of(FieldPath.parse("a.b.c"))));
  }

  @Test
  public void testHasAncestorOperatorWithInvalidNonChildEmbeddedPath() {
    Operator hasAncestorOperator =
        OperatorBuilder.hasAncestor()
            .ancestorPath("a.b")
            .operator(OperatorBuilder.text().path("a.b.c").query("query").build())
            .build();
    OperatorEmbeddedRootValidator validator =
        new OperatorEmbeddedRootValidator(BsonParseContext.root());
    TestUtils.assertThrows(
        "hasAncestor.ancestorPath 'a.b' must be a parent of returnScope.path 'a'",
        BsonParseException.class,
        () -> validator.validate(hasAncestorOperator, Optional.of(FieldPath.parse("a"))));
  }

  @Test
  public void testHasAncestorOperatorWithEmptyEmbeddedRootValid() {
    Operator hasAncestorOperator =
        OperatorBuilder.hasAncestor()
            .ancestorPath("a.b")
            .operator(OperatorBuilder.text().path("a.b.c").query("query").build())
            .build();
    OperatorEmbeddedRootValidator validator =
        new OperatorEmbeddedRootValidator(BsonParseContext.root());
    TestUtils.assertThrows(
        "hasAncestor operator requires embeddedDocument.path, hasAncestor.ancestorPath, "
            + "or returnScope.path to be set.",
        BsonParseException.class,
        () -> validator.validate(hasAncestorOperator, Optional.empty()));
  }

  @Test
  public void testHasAncestorOperatorWithValidScore() throws Exception {
    Operator hasAncestorOperator =
        OperatorBuilder.hasAncestor()
            .ancestorPath("a.b")
            .operator(OperatorBuilder.text().path("a.b.c").query("query").build())
            .score(
                ScoreBuilder.function()
                    .expression(
                        MultiplyExpressionBuilder.builder()
                            .arg(PathExpressionBuilder.builder().value("a.b.d").build())
                            .arg(ScoreExpressionBuilder.builder().build())
                            .build())
                    .build())
            .build();
    OperatorEmbeddedRootValidator validator =
        new OperatorEmbeddedRootValidator(BsonParseContext.root());
    validator.validate(hasAncestorOperator, Optional.of(FieldPath.parse("a.b.c")));
  }

  @Test
  public void testHasAncestorOperatorWithInvalidScore() {
    Operator hasAncestorOperator =
        OperatorBuilder.hasAncestor()
            .ancestorPath("a.b")
            .operator(OperatorBuilder.text().path("a.b.c").query("query").build())
            .score(
                ScoreBuilder.function()
                    .expression(
                        MultiplyExpressionBuilder.builder()
                            .arg(PathExpressionBuilder.builder().value("b").build())
                            .arg(ScoreExpressionBuilder.builder().build())
                            .build())
                    .build())
            .build();
    OperatorEmbeddedRootValidator validator =
        new OperatorEmbeddedRootValidator(BsonParseContext.root());
    TestUtils.assertThrows(
        "hasAncestor operator score path expression 'b' must "
            + "be a descendant of hasAncestor.ancestorPath 'a.b'",
        BsonParseException.class,
        () -> validator.validate(hasAncestorOperator, Optional.of(FieldPath.parse("a.b.c"))));
  }

  @Test
  public void testHasRootOperator() throws Exception {
    Operator hasRootOperator =
        OperatorBuilder.hasRoot()
            .operator(OperatorBuilder.text().path("a.b.c").query("query").build())
            .build();
    OperatorEmbeddedRootValidator validator =
        new OperatorEmbeddedRootValidator(BsonParseContext.root());
    validator.validate(hasRootOperator, Optional.of(FieldPath.parse("a.b.c")));
  }

  @Test
  public void testHasRootOperatorWithoutEmbeddedRoot() {
    Operator hasRootOperator =
        OperatorBuilder.hasRoot()
            .operator(OperatorBuilder.text().path("a.b.c").query("query").build())
            .build();
    OperatorEmbeddedRootValidator validator =
        new OperatorEmbeddedRootValidator(BsonParseContext.root());
    TestUtils.assertThrows(
        "hasRoot operator requires embeddedDocument.path, hasAncestor.ancestorPath, or"
            + " returnScope.path to be set.",
        BsonParseException.class,
        () -> validator.validate(hasRootOperator, Optional.empty()));
  }

  @Test
  public void testSimpleCompoundOperator() throws Exception {
    Operator compoundOperator =
        OperatorBuilder.compound()
            .should(OperatorBuilder.text().path("a.b").query("query").build())
            .should(OperatorBuilder.text().path("a.c").query("query").build())
            .build();
    OperatorEmbeddedRootValidator validator =
        new OperatorEmbeddedRootValidator(BsonParseContext.root());
    validator.validate(compoundOperator, Optional.empty());
  }

  @Test
  public void testCompoundOperatorWithReturnScope() throws Exception {
    Operator compoundOperator =
        OperatorBuilder.compound()
            .should(OperatorBuilder.text().path("a.b").query("query").build())
            .should(OperatorBuilder.text().path("a.c").query("query").build())
            .build();
    OperatorEmbeddedRootValidator validator =
        new OperatorEmbeddedRootValidator(BsonParseContext.root());
    validator.validate(compoundOperator, Optional.of(FieldPath.parse("a")));
  }

  @Test
  public void testCompoundOperatorWithInvalidReturnScope() {
    Operator compoundOperator =
        OperatorBuilder.compound()
            .should(OperatorBuilder.text().path("a.b").query("query").build())
            .should(OperatorBuilder.text().path("a.c").query("query").build())
            .build();
    OperatorEmbeddedRootValidator validator =
        new OperatorEmbeddedRootValidator(BsonParseContext.root());
    TestUtils.assertThrows(
        "text operator path 'a.b' must be a descendant of returnScope.path 'a.b.c'",
        BsonParseException.class,
        () -> validator.validate(compoundOperator, Optional.of(FieldPath.parse("a.b.c"))));
  }

  @Test
  public void testCompoundOperatorWithEmbeddedDocumentOperator() throws Exception {
    Operator compoundOperator =
        OperatorBuilder.compound()
            .should(
                OperatorBuilder.embeddedDocument()
                    .path("a.b")
                    .operator(OperatorBuilder.text().path("a.b.c").query("query").build())
                    .build())
            .should(OperatorBuilder.text().path("a.d").query("query").build())
            .build();
    OperatorEmbeddedRootValidator validator =
        new OperatorEmbeddedRootValidator(BsonParseContext.root());
    validator.validate(compoundOperator, Optional.of(FieldPath.parse("a")));
  }

  @Test
  public void testCompoundOperatorWithInvalidEmbeddedDocumentOperator() {
    Operator compoundOperator =
        OperatorBuilder.compound()
            .should(
                OperatorBuilder.embeddedDocument()
                    .path("a.b")
                    .operator(OperatorBuilder.text().path("a.b.c").query("query").build())
                    .build())
            .should(OperatorBuilder.text().path("a.d").query("query").build())
            .build();
    OperatorEmbeddedRootValidator validator =
        new OperatorEmbeddedRootValidator(BsonParseContext.root());
    TestUtils.assertThrows(
        "embeddedDocument operator path 'a.b' must be a descendant of returnScope.path 'a.b.c.d'",
        BsonParseException.class,
        () -> validator.validate(compoundOperator, Optional.of(FieldPath.parse("a.b.c.d"))));
  }

  @Test
  public void testCompoundOperatorWithEmbeddedDocumentAndScoring() throws Exception {
    Operator compoundOperator =
        OperatorBuilder.compound()
            .should(
                OperatorBuilder.embeddedDocument()
                    .path("a.b")
                    .operator(OperatorBuilder.text().path("a.b.c").query("query").build())
                    .score(
                        ScoreBuilder.function()
                            .expression(
                                MultiplyExpressionBuilder.builder()
                                    .arg(ScoreExpressionBuilder.builder().build())
                                    .arg(PathExpressionBuilder.builder().value("a.b.d").build())
                                    .build())
                            .build())
                    .build())
            .should(OperatorBuilder.text().path("a.d").query("query").build())
            .build();
    OperatorEmbeddedRootValidator validator =
        new OperatorEmbeddedRootValidator(BsonParseContext.root());
    validator.validate(compoundOperator, Optional.of(FieldPath.parse("a")));
  }

  @Test
  public void testCompoundOperatorWithInvalidEmbeddedDocumentOperatorAndScoring() {
    Operator compoundOperator =
        OperatorBuilder.compound()
            .should(
                OperatorBuilder.embeddedDocument()
                    .path("a.b")
                    .operator(OperatorBuilder.text().path("a.b.c").query("query").build())
                    .score(
                        ScoreBuilder.function()
                            .expression(
                                MultiplyExpressionBuilder.builder()
                                    .arg(ScoreExpressionBuilder.builder().build())
                                    .arg(PathExpressionBuilder.builder().value("b").build())
                                    .build())
                            .build())
                    .build())
            .should(OperatorBuilder.text().path("a.d").query("query").build())
            .build();
    OperatorEmbeddedRootValidator validator =
        new OperatorEmbeddedRootValidator(BsonParseContext.root());
    TestUtils.assertThrows(
        "embeddedDocument operator score path expression 'b' "
            + "must be a descendant of returnScope.path 'a'",
        BsonParseException.class,
        () -> validator.validate(compoundOperator, Optional.of(FieldPath.parse("a"))));
  }

  @Test
  public void testCompoundOperatorWithValidHasAncestor() throws Exception {
    Operator compoundOperator =
        OperatorBuilder.compound()
            .must(
                OperatorBuilder.hasAncestor()
                    .ancestorPath("a.b")
                    .operator(OperatorBuilder.text().path("a.b.c").query("query").build())
                    .build())
            .should(OperatorBuilder.text().path("a.b.c.d").query("query").build())
            .build();
    OperatorEmbeddedRootValidator validator =
        new OperatorEmbeddedRootValidator(BsonParseContext.root());
    validator.validate(compoundOperator, Optional.of(FieldPath.parse("a.b.c")));
  }

  @Test
  public void testCompoundOperatorWithInvalidHasAncestor() {
    Operator compoundOperator =
        OperatorBuilder.compound()
            .must(
                OperatorBuilder.hasAncestor()
                    .ancestorPath("a.b")
                    .operator(OperatorBuilder.text().path("a.b.c").query("query").build())
                    .build())
            .should(OperatorBuilder.text().path("a.d").query("query").build())
            .build();
    OperatorEmbeddedRootValidator validator =
        new OperatorEmbeddedRootValidator(BsonParseContext.root());
    TestUtils.assertThrows(
        "text operator path 'a.d' must be a descendant of returnScope.path 'a.b.c'",
        BsonParseException.class,
        () -> validator.validate(compoundOperator, Optional.of(FieldPath.parse("a.b.c"))));
  }

  @Test
  public void testCompoundOperatorWithHasAncestorWithMissingEmbeddedRoot() {
    Operator compoundOperator =
        OperatorBuilder.compound()
            .must(
                OperatorBuilder.hasAncestor()
                    .ancestorPath("a.b")
                    .operator(OperatorBuilder.text().path("a.b.c").query("query").build())
                    .build())
            .should(OperatorBuilder.text().path("a.").query("query").build())
            .build();
    OperatorEmbeddedRootValidator validator =
        new OperatorEmbeddedRootValidator(BsonParseContext.root());
    TestUtils.assertThrows(
        "hasAncestor operator requires embeddedDocument.path, hasAncestor.ancestorPath,"
            + " or returnScope.path to be set.",
        BsonParseException.class,
        () -> validator.validate(compoundOperator, Optional.empty()));
  }

  @Test
  public void testNestedHasAncestorOperator() throws Exception {
    Operator hasAncestorOperator =
        OperatorBuilder.hasAncestor()
            .ancestorPath("a.b")
            .operator(
                OperatorBuilder.hasAncestor()
                    .ancestorPath("a")
                    .operator(OperatorBuilder.text().path("a.b.c.d").query("query").build())
                    .build())
            .build();
    OperatorEmbeddedRootValidator validator =
        new OperatorEmbeddedRootValidator(BsonParseContext.root());
    validator.validate(hasAncestorOperator, Optional.of(FieldPath.parse("a.b.c.d")));
  }

  @Test
  public void testNestedInvalidHasAncestorOperator() {
    Operator hasAncestorOperator =
        OperatorBuilder.hasAncestor()
            .ancestorPath("a.b")
            .operator(
                OperatorBuilder.hasAncestor()
                    .ancestorPath("a.b.c")
                    .operator(OperatorBuilder.text().path("a.b.c.d").query("query").build())
                    .build())
            .build();
    OperatorEmbeddedRootValidator validator =
        new OperatorEmbeddedRootValidator(BsonParseContext.root());
    TestUtils.assertThrows(
        "hasAncestor.ancestorPath 'a.b.c' must be a parent of hasAncestor.ancestorPath 'a.b'",
        BsonParseException.class,
        () -> validator.validate(hasAncestorOperator, Optional.of(FieldPath.parse("a.b.c"))));
  }

  @Test
  public void testNestedCompoundEmbeddedHasAncestorOperator() throws Exception {
    Operator hasAncestorOperator =
        OperatorBuilder.hasAncestor()
            .ancestorPath("a.b")
            .operator(
                OperatorBuilder.compound()
                    .must(
                        OperatorBuilder.embeddedDocument()
                            .path("a.b.c")
                            .operator(OperatorBuilder.text().path("a.b.c.d").query("query").build())
                            .build())
                    .should(OperatorBuilder.text().path("a.b.e").query("query").build())
                    .build())
            .build();
    OperatorEmbeddedRootValidator validator =
        new OperatorEmbeddedRootValidator(BsonParseContext.root());
    validator.validate(hasAncestorOperator, Optional.of(FieldPath.parse("a.b.c.d")));
  }

  @Test
  public void testJoinCountExceedingMaxAllowed() {
    Operator hasAncestorOperator =
        OperatorBuilder.hasAncestor() // Join 1
            .ancestorPath("a.b")
            .operator(
                OperatorBuilder.compound()
                    .must(
                        OperatorBuilder.embeddedDocument() // join 2
                            .path("a.b.c")
                            .operator(
                                OperatorBuilder.hasAncestor() // join 3
                                    .ancestorPath("a.b")
                                    .operator(
                                        OperatorBuilder.embeddedDocument() // join 4
                                            .path("a.b.d.e")
                                            .operator(
                                                OperatorBuilder.hasAncestor() // join 5
                                                    .ancestorPath("a.b.d")
                                                    .operator(
                                                        OperatorBuilder.text()
                                                            .path("a.b.d.f")
                                                            .query("query")
                                                            .build())
                                                    .build())
                                            .build())
                                    .build())
                            .build())
                    .should(OperatorBuilder.text().path("a.b.e").query("query").build())
                    .build())
            .build();
    OperatorEmbeddedRootValidator validator =
        new OperatorEmbeddedRootValidator(BsonParseContext.root());
    TestUtils.assertThrows(
        "Query must contain less than 4 occurrences of hasAncestor + hasRoot + "
            + "embeddedDocuments.",
        BsonParseException.class,
        () -> validator.validate(hasAncestorOperator, Optional.of(FieldPath.parse("a.b.c.d"))));
  }
}
