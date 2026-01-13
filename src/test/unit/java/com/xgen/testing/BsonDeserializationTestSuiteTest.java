package com.xgen.testing;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.testing.BsonDeserializationTestSuite.fromValue;

import com.xgen.testing.BsonDeserializationTestSuite.TestSpecWrapper;
import java.util.Collection;
import org.junit.Assert;
import org.junit.Test;

public class BsonDeserializationTestSuiteTest {

  private static final String SUITE_NAME = "deserialization-suite";

  private static final BsonDeserializationTestSuite<String> TEST_SUITE =
      fromValue("src/test/unit/resources", SUITE_NAME, (u, v) -> v.asString().getValue());

  @Test
  public void loadAppendsInvalidExamples() {
    var example = BsonDeserializationTestSuite.TestSpec.valid("string value", "example");
    var empty = BsonDeserializationTestSuite.TestSpec.valid("empty", "");

    Collection<TestSpecWrapper<String>> specs = TEST_SUITE.withExamples(example, empty);

    assertThat(specs).hasSize(4);
    assertThat(specs.stream().map(TestSpecWrapper::getName))
        .containsExactly("string value", "empty", "wrong type", "no value");
    assertThat(specs.stream().filter(TestSpecWrapper::isValid)).hasSize(2);
  }

  @Test
  public void loadDetectsMissingValidExample() {
    var example = BsonDeserializationTestSuite.TestSpec.valid("string value", "example");

    Assert.assertThrows(AssertionError.class, () -> TEST_SUITE.withExamples(example));
  }
}
