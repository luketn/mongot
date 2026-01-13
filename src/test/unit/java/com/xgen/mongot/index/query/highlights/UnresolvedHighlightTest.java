package com.xgen.mongot.index.query.highlights;

import static com.xgen.testing.BsonDeserializationTestSuite.TestSpec;
import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;

import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonDeserializationTestSuite.TestSpecWrapper;
import com.xgen.testing.BsonDeserializationTestSuite.ValidSpec;
import com.xgen.testing.mongot.index.path.string.UnresolvedStringPathBuilder;
import com.xgen.testing.mongot.index.query.highlights.UnresolvedHighlightBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class UnresolvedHighlightTest {

  private static final String SUITE_NAME = "highlight";
  private static final BsonDeserializationTestSuite<UnresolvedHighlight> TEST_SUITE =
      fromDocument(
          "src/test/unit/resources/index/query/highlights/",
          SUITE_NAME,
          UnresolvedHighlight::fromBson);

  private final TestSpecWrapper<UnresolvedHighlight> testSpec;

  public UnresolvedHighlightTest(TestSpecWrapper<UnresolvedHighlight> testSpec) {
    this.testSpec = testSpec;
  }

  @Parameterized.Parameters(name = "{0}")
  public static Iterable<TestSpecWrapper<UnresolvedHighlight>> data() {
    return TEST_SUITE.withExamples(
        simple(), explicitMaxNumPassagesAndMaxCharsToExamine(), pathWithMulti(), wildcardPath());
  }

  @Test
  public void runTest() throws Exception {
    TEST_SUITE.runTest(this.testSpec);
  }

  private static ValidSpec<UnresolvedHighlight> simple() {
    return TestSpec.valid("simple", UnresolvedHighlightBuilder.builder().path("title").build());
  }

  private static ValidSpec<UnresolvedHighlight> explicitMaxNumPassagesAndMaxCharsToExamine() {
    return TestSpec.valid(
        "explicit maxNumPassages and maxCharsToExamine",
        UnresolvedHighlightBuilder.builder()
            .path("title")
            .maxNumPassages(1)
            .maxCharsToExamine(2)
            .build());
  }

  private static ValidSpec<UnresolvedHighlight> pathWithMulti() {
    return TestSpec.valid(
        "path with multi",
        UnresolvedHighlightBuilder.builder()
            .path(UnresolvedStringPathBuilder.withMulti("title", "keyword"))
            .build());
  }

  private static ValidSpec<UnresolvedHighlight> wildcardPath() {
    return TestSpec.valid(
        "wildcardPath",
        UnresolvedHighlightBuilder.builder()
            .path(UnresolvedStringPathBuilder.wildcardPath("des*"))
            .build());
  }
}
