package com.xgen.mongot.util.mongodb;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.xgen.mongot.util.Check;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class MongoCredentials {
  // These regexes were divined based on a combination of
  // https://github.com/mongodb/mongo/blob/r8.1.0-alpha/src/mongo/db/auth/security_file.cpp#L60-L71
  //  - character set
  //  - whitespace set
  // https://www.mongodb.com/docs/manual/core/security-internal-authentication/#key-requirements
  //  - length requirements
  private static final Pattern VALID_KEYFILE_CHARS = Pattern.compile("[a-zA-Z0-9+/=]{6,1024}");
  private static final Pattern DROPPED_WHITESPACE = Pattern.compile("[\\x09-\\x0D ]");

  // keys can also be specified in a yaml file
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory());

  public static final String SYSTEM_USER = "__system";

  public static String readKeyFile(Path keyFile) throws IOException {
    Check.checkArg(keyFile.toFile().exists(), "Key file %s does not exist", keyFile);
    Check.checkArg(keyFile.toFile().length() <= 4096, "Key file %s is unexpectedly long", keyFile);

    return parseToList(keyFile).getFirst();
  }

  private static List<String> parseToList(Path keyFile) throws IOException {
    var node = OBJECT_MAPPER.readTree(keyFile.toFile());
    if (node instanceof ArrayNode arrayNode) {
      Check.checkArg(
          arrayNode.size() == 1 || arrayNode.size() == 2,
          "Only two keys in the keyfile are supported");
      var keys = new ArrayList<String>();
      if (!(arrayNode.get(0) instanceof TextNode textNode0)) {
        throw new IllegalArgumentException("First key file element is not a string.");
      }
      keys.add(validateSingleKey("first key", textNode0.textValue()));
      if (arrayNode.size() == 2) {
        if (!(arrayNode.get(1) instanceof TextNode textNode1)) {
          throw new IllegalArgumentException("Second key file element is not a string.");
        }
        keys.add(validateSingleKey("second key", textNode1.textValue()));
      }
      return List.copyOf(keys);
    }
    if (node instanceof TextNode textNode0) {
      return List.of(validateSingleKey("key", textNode0.textValue()));
    }

    throw new IllegalArgumentException("Key file must contain a string or yaml sequence.");
  }

  private static String validateSingleKey(String identifier, String value) {
    var stripped = DROPPED_WHITESPACE.matcher(value).replaceAll("");

    Check.checkArg(stripped.length() >= 6, "%s must be at least 6 characters long", identifier);
    Check.checkArg(stripped.length() <= 1024, "%s must not exceed 1024 characters", identifier);
    Check.checkArg(
        VALID_KEYFILE_CHARS.matcher(stripped).matches(),
        "%s contains invalid characters",
        identifier);

    return stripped;
  }
}
