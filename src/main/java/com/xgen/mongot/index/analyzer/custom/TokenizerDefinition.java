package com.xgen.mongot.index.analyzer.custom;

import com.xgen.mongot.index.analyzer.attributes.TokenStreamTypeAware;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import org.bson.BsonDocument;

public abstract class TokenizerDefinition implements DocumentEncodable, TokenStreamTypeAware {
  static class Fields {
    static final Field.Required<Type> TYPE =
        Field.builder("type").enumField(Type.class).asCamelCase().required();
  }

  public enum Type {
    EDGE_GRAM,
    KEYWORD,
    N_GRAM,
    REGEX_CAPTURE_GROUP,
    REGEX_SPLIT,
    STANDARD,
    UAX_URL_EMAIL,
    WHITESPACE
  }

  public abstract Type getType();

  abstract BsonDocument tokenizerToBson();

  @Override
  public abstract boolean equals(Object other);

  @Override
  public abstract int hashCode();

  @Override
  public final BsonDocument toBson() {
    BsonDocument doc = BsonDocumentBuilder.builder().field(Fields.TYPE, getType()).build();
    doc.putAll(tokenizerToBson());
    return doc;
  }

  /** Deserialize a BSON document into a new CustomTokenizerDefinition. */
  public static TokenizerDefinition fromBson(DocumentParser parser) throws BsonParseException {
    var type = parser.getField(Fields.TYPE).unwrap();
    return switch (type) {
      case EDGE_GRAM -> EdgeGramTokenizerDefinition.fromBson(parser);
      case KEYWORD -> new KeywordTokenizerDefinition();
      case N_GRAM -> NGramTokenizerDefinition.fromBson(parser);
      case REGEX_CAPTURE_GROUP -> RegexCaptureGroupTokenizerDefinition.fromBson(parser);
      case REGEX_SPLIT -> RegexSplitTokenizerDefinition.fromBson(parser);
      case STANDARD -> StandardTokenizerDefinition.fromBson(parser);
      case UAX_URL_EMAIL -> UaxUrlEmailTokenizerDefinition.fromBson(parser);
      case WHITESPACE -> WhitespaceTokenizerDefinition.fromBson(parser);
    };
  }

  public EdgeGramTokenizerDefinition asEdgeGramTokenizerDefinition() {
    Check.expectedType(Type.EDGE_GRAM, this.getType());
    return (EdgeGramTokenizerDefinition) this;
  }

  public KeywordTokenizerDefinition asKeywordTokenizerDefinition() {
    Check.expectedType(Type.KEYWORD, this.getType());
    return (KeywordTokenizerDefinition) this;
  }

  public NGramTokenizerDefinition asNGramTokenizerDefinition() {
    Check.expectedType(Type.N_GRAM, this.getType());
    return (NGramTokenizerDefinition) this;
  }

  public RegexCaptureGroupTokenizerDefinition asRegexCaptureGroupTokenizerDefinition() {
    Check.expectedType(Type.REGEX_CAPTURE_GROUP, this.getType());
    return (RegexCaptureGroupTokenizerDefinition) this;
  }

  public RegexSplitTokenizerDefinition asRegexSplitTokenizerDefinition() {
    Check.expectedType(Type.REGEX_SPLIT, this.getType());
    return (RegexSplitTokenizerDefinition) this;
  }

  public StandardTokenizerDefinition asStandardTokenizerDefinition() {
    Check.expectedType(Type.STANDARD, this.getType());
    return (StandardTokenizerDefinition) this;
  }

  public UaxUrlEmailTokenizerDefinition asUaxUrlEmailTokenizerDefinition() {
    Check.expectedType(Type.UAX_URL_EMAIL, this.getType());
    return (UaxUrlEmailTokenizerDefinition) this;
  }

  public WhitespaceTokenizerDefinition asWhitespaceTokenizerDefinition() {
    Check.expectedType(Type.WHITESPACE, this.getType());
    return (WhitespaceTokenizerDefinition) this;
  }
}
