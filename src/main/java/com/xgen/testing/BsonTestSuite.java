package com.xgen.testing;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;

/**
 * Represents a suite of tests described in a JSON file, parsed into BSON. Note that we parse into
 * BSON since we can trivially use Document::toJson to get the JSON representation.
 *
 * <p>The test file should be a JSON file containing a single object. That object should have two
 * fields, valid and invalid. These are both arrays of objects, each of which should contain a
 * description string field, and a value field containing the actual value to be testing.
 */
public class BsonTestSuite {

  BsonTestSuite(List<TestCase> valid, List<TestCase> invalid) {
    this.valid = valid;
    this.invalid = invalid;
  }

  public final List<TestCase> valid;
  public final List<TestCase> invalid;

  public static class TestCase {
    private final String description;
    private final BsonValue value;

    public TestCase(String description, BsonValue value) {
      this.description = description;
      this.value = value;
    }

    public BsonValue getValue() {
      return this.value;
    }

    public String getDescription() {
      return this.description;
    }

    /** Returns a JSON string representation of the value. */
    public String valueAsJson() {
      return bsonValueToJson(this.value);
    }

    public BsonDocument valueAsBsonDocument() {
      return this.value.asDocument();
    }

    private static String bsonValueToJson(BsonValue value) {
      return switch (value.getBsonType()) {
        case BOOLEAN -> Boolean.toString(value.asBoolean().getValue());
        case NULL -> "null";
        case DOCUMENT -> value.asDocument().toJson();
        case ARRAY ->
            String.format(
                "[%s]",
                String.join(
                    ", ",
                    value.asArray().stream()
                        .map(TestCase::bsonValueToJson)
                        .toArray(String[]::new)));
        default ->
            throw new AssertionError(
                String.format("unsupported test case value type %s", value.getBsonType()));
      };
    }
  }

  /** Load a file named [name].json in the directory [path]. */
  public static BsonTestSuite load(String path, String name) {
    byte[] encoded;
    try {
      encoded = Files.readAllBytes(Paths.get(path, name + ".json"));
      String json = new String(encoded);
      BsonDocument document = BsonDocument.parse(json);

      List<TestCase> valid = parseTestCases(document.getArray("valid"));
      List<TestCase> invalid = parseTestCases(document.getArray("invalid"));
      return new BsonTestSuite(valid, invalid);
    } catch (Throwable t) {
      throw new RuntimeException(
          String.format("error instantiating BsonTestSuite for %s: %s", name, t.getMessage()));
    }
  }

  private static List<TestCase> parseTestCases(BsonArray testCasesArray) {
    List<TestCase> testCases = new ArrayList<>(testCasesArray.size());
    for (BsonValue value : testCasesArray) {
      BsonDocument testCase = value.asDocument();
      testCases.add(
          new TestCase(testCase.getString("description").getValue(), testCase.get("value")));
    }

    return testCases;
  }
}
