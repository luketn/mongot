package com.xgen.testing;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonValue;

public class JsonTestSuite {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final ObjectMapper YAML_OBJECT_MAPPER = new ObjectMapper(new YAMLFactory());

  public static class TestCase {

    private final String description;
    private final JsonNode value;
    private final String errorMessageContains;

    @JsonCreator
    public TestCase(
        @JsonProperty(value = "description", required = true) String description,
        @JsonProperty(value = "value", required = true) JsonNode value,
        @JsonProperty(value = "errorMessageContains", required = false)
            String errorMessageContains) {
      this.description = description;
      this.value = value;
      this.errorMessageContains = Optional.ofNullable(errorMessageContains).orElse("");
    }

    public String getDescription() {
      return this.description;
    }

    /** Only here for JSON serialization. */
    public JsonNode getValue() {
      return this.value;
    }

    public String getErrorMessageContains() {
      return this.errorMessageContains;
    }

    /** Returns the value as a BSON value. */
    @JsonIgnore
    public BsonValue getValueAsBsonValue() {
      // org.bson only supplies utilities for parsing BsonDocuments from JSON, but we want the value
      // to be able to be any JSON type.
      // So here we serialize the whole test case, since we know it's a document, then parse that
      // value into a BsonDocument, and return the BsonValue from the "value" key.
      String testCaseJson;
      try {
        testCaseJson = OBJECT_MAPPER.writeValueAsString(this);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }

      BsonDocument testCaseDocument = BsonDocument.parse(testCaseJson);
      return testCaseDocument.get("value");
    }

    /** Returns the value as a JSON string. */
    @JsonIgnore
    public String getValueAsJsonString() {
      return this.value.toString();
    }

    /** Returns the value as a YAML string. */
    @JsonIgnore
    public String getValueAsYamlString() {
      try {
        return YAML_OBJECT_MAPPER.writeValueAsString(this.value);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private final List<TestCase> valid;
  private final List<TestCase> invalid;

  @JsonCreator
  public JsonTestSuite(
      @JsonProperty("valid") List<TestCase> valid,
      @JsonProperty("invalid") List<TestCase> invalid) {
    this.valid = Optional.ofNullable(valid).orElseGet(ArrayList::new);
    this.invalid = Optional.ofNullable(invalid).orElseGet(ArrayList::new);
  }

  public List<TestCase> getValid() {
    return this.valid;
  }

  public List<TestCase> getInvalid() {
    return this.invalid;
  }

  /** Load a file named [name].json in the directory [path]. */
  public static JsonTestSuite load(String path, String name) {
    try (BufferedReader reader = Files.newBufferedReader(Paths.get(path, name + ".json"))) {
      return OBJECT_MAPPER.readValue(reader, JsonTestSuite.class);
    } catch (Throwable t) {
      throw new RuntimeException(
          String.format("error instantiating JsonTestSuite for %s", name), t);
    }
  }
}
