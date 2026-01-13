package com.xgen.mongot.index.analyzer.custom;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.bson.BsonDocument;

public class HtmlStripCharFilterDefinition extends CharFilterDefinition {
  static class Fields {
    static final Field.WithDefault<List<String>> IGNORED_TAGS =
        Field.builder("ignoredTags")
            .stringField()
            .asList()
            .mustBeUnique()
            .optional()
            .withDefault(List.of());
  }

  public final Set<String> ignoredTags;

  public HtmlStripCharFilterDefinition(Set<String> ignoredTags) {
    this.ignoredTags = ignoredTags;
  }

  public static HtmlStripCharFilterDefinition fromBson(DocumentParser parser)
      throws BsonParseException {
    return new HtmlStripCharFilterDefinition(
        Sets.newHashSet(parser.getField(Fields.IGNORED_TAGS).unwrap()));
  }

  @Override
  public Type getType() {
    return Type.HTML_STRIP;
  }

  @Override
  BsonDocument charFilterToBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.IGNORED_TAGS, Lists.newArrayList(this.ignoredTags))
        .build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    HtmlStripCharFilterDefinition that = (HtmlStripCharFilterDefinition) o;
    return this.ignoredTags.equals(that.ignoredTags);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.ignoredTags);
  }
}
