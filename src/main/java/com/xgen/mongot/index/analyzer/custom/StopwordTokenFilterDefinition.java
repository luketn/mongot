package com.xgen.mongot.index.analyzer.custom;

import com.xgen.mongot.index.analyzer.attributes.TokenStreamTransformationStage;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.List;
import java.util.Objects;
import org.bson.BsonDocument;

public class StopwordTokenFilterDefinition extends TokenFilterDefinition
    implements TokenStreamTransformationStage.OutputsSameTypeAsInput {
  private static class Fields {
    private static final Field.Required<List<String>> TOKENS =
        Field.builder("tokens").stringField().asList().mustNotBeEmpty().required();

    private static final Field.WithDefault<Boolean> IGNORE_CASE =
        Field.builder("ignoreCase").booleanField().optional().withDefault(true);
  }

  public final List<String> tokens;
  public final boolean ignoreCase;

  public StopwordTokenFilterDefinition(List<String> tokens, boolean ignoreCase) {
    this.tokens = tokens;
    this.ignoreCase = ignoreCase;
  }

  public static StopwordTokenFilterDefinition fromBson(DocumentParser parser)
      throws BsonParseException {
    return new StopwordTokenFilterDefinition(
        parser.getField(Fields.TOKENS).unwrap(), parser.getField(Fields.IGNORE_CASE).unwrap());
  }

  @Override
  BsonDocument tokenFilterToBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.TOKENS, this.tokens)
        .field(Fields.IGNORE_CASE, this.ignoreCase)
        .build();
  }

  @Override
  public Type getType() {
    return Type.STOPWORD;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    StopwordTokenFilterDefinition that = (StopwordTokenFilterDefinition) o;
    return Objects.equals(this.tokens, that.tokens) && this.ignoreCase == that.ignoreCase;
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.tokens, this.ignoreCase);
  }
}
