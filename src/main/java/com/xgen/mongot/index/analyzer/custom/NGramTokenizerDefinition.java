package com.xgen.mongot.index.analyzer.custom;

import com.xgen.mongot.index.analyzer.attributes.TokenStreamTypeAware;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Objects;
import org.bson.BsonDocument;

public class NGramTokenizerDefinition extends TokenizerDefinition
    implements TokenStreamTypeAware.Graph {
  static class Fields {
    static final Field.Required<Integer> MIN_GRAM =
        Field.builder("minGram").intField().mustBeNonNegative().required();

    static final Field.Required<Integer> MAX_GRAM =
        Field.builder("maxGram").intField().mustBeNonNegative().required();
  }

  public final int minGram;
  public final int maxGram;

  public NGramTokenizerDefinition(int minGram, int maxGram) {
    this.minGram = minGram;
    this.maxGram = maxGram;
  }

  /** Deserialize an NGramTokenizerDefinition. */
  public static NGramTokenizerDefinition fromBson(DocumentParser parser) throws BsonParseException {
    int minGram = parser.getField(Fields.MIN_GRAM).unwrap();
    int maxGram = parser.getField(Fields.MAX_GRAM).unwrap();

    if (minGram > maxGram) {
      parser.getContext().handleSemanticError("minGram must not be greater than maxGram");
    }

    return new NGramTokenizerDefinition(minGram, maxGram);
  }

  @Override
  public Type getType() {
    return Type.N_GRAM;
  }

  @Override
  BsonDocument tokenizerToBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.MIN_GRAM, this.minGram)
        .field(Fields.MAX_GRAM, this.maxGram)
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

    NGramTokenizerDefinition that = (NGramTokenizerDefinition) o;
    return this.minGram == that.minGram && this.maxGram == that.maxGram;
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.minGram, this.maxGram);
  }
}
