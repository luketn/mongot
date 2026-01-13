package com.xgen.mongot.index.analyzer.custom;

import com.xgen.mongot.index.analyzer.attributes.TokenStreamTypeAware;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import org.bson.BsonDocument;

public class RegexCaptureGroupTokenizerDefinition extends TokenizerDefinition
    implements TokenStreamTypeAware.Stream {
  static class Fields {
    static final Field.Required<String> PATTERN = Field.builder("pattern").stringField().required();

    static final Field.Required<Integer> GROUP =
        Field.builder("group").intField().mustBeNonNegative().required();
  }

  public final Pattern pattern;
  public final int group;

  public RegexCaptureGroupTokenizerDefinition(Pattern pattern, int group) {
    this.pattern = pattern;
    this.group = group;
  }

  public static RegexCaptureGroupTokenizerDefinition fromBson(DocumentParser parser)
      throws BsonParseException {
    String strPattern = parser.getField(Fields.PATTERN).unwrap();

    Pattern pattern;

    try {
      // check that the pattern is valid
      pattern = Pattern.compile(strPattern);
    } catch (PatternSyntaxException ex) {
      return parser.getContext().handleSemanticError(ex.getMessage());
    }

    // check that the group is valid

    int group = parser.getField(Fields.GROUP).unwrap();
    // create a matcher with "" as the text to match the pattern against,
    // as we only need the group count.
    Matcher matcher = pattern.matcher("");
    if (group >= 0 && group > matcher.groupCount()) {
      parser
          .getContext()
          .handleSemanticError(
              String.format(
                  "invalid group specified: pattern only has: %s capturing groups",
                  matcher.groupCount()));
    }

    return new RegexCaptureGroupTokenizerDefinition(pattern, group);
  }

  @Override
  public Type getType() {
    return Type.REGEX_CAPTURE_GROUP;
  }

  @Override
  BsonDocument tokenizerToBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.PATTERN, this.pattern.pattern())
        .field(Fields.GROUP, this.group)
        .build();
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }

    if (!(other instanceof RegexCaptureGroupTokenizerDefinition)) {
      return false;
    }

    RegexCaptureGroupTokenizerDefinition otherDefinition =
        (RegexCaptureGroupTokenizerDefinition) other;

    // Pattern::equals() currently only tests identity - compare underlying string instead
    return Objects.equals(this.pattern.pattern(), otherDefinition.pattern.pattern())
        && Objects.equals(this.group, otherDefinition.group);
  }

  @Override
  public int hashCode() {
    // Pattern::hashCode() currently only tests identity - compare underlying string instead
    return Objects.hash(this.pattern.pattern(), this.group);
  }
}
