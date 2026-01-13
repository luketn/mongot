package com.xgen.mongot.index.analyzer.custom;

import com.xgen.mongot.index.analyzer.attributes.TokenStreamTransformationStage;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Objects;
import org.bson.BsonDocument;

public class DaitchMokotoffSoundexTokenFilterDefinition extends TokenFilterDefinition
    implements TokenStreamTransformationStage.AnyToGraph {
  private static class Fields {
    private static final Field.WithDefault<OriginalTokens> ORIGINAL_TOKENS =
        Field.builder("originalTokens")
            .enumField(OriginalTokens.class)
            .asCamelCase()
            .optional()
            .withDefault(OriginalTokens.INCLUDE);
  }

  public final OriginalTokens outputOption;

  public DaitchMokotoffSoundexTokenFilterDefinition(OriginalTokens outputOption) {
    this.outputOption = outputOption;
  }

  @Override
  BsonDocument tokenFilterToBson() {
    return BsonDocumentBuilder.builder().field(Fields.ORIGINAL_TOKENS, this.outputOption).build();
  }

  /** Deserialize a DaitchMokotoffSoundexTokenFilterDefinition. */
  public static DaitchMokotoffSoundexTokenFilterDefinition fromBson(DocumentParser parser)
      throws BsonParseException {
    return new DaitchMokotoffSoundexTokenFilterDefinition(
        parser.getField(Fields.ORIGINAL_TOKENS).unwrap());
  }

  @Override
  public Type getType() {
    return Type.DAITCH_MOKOTOFF_SOUNDEX;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    DaitchMokotoffSoundexTokenFilterDefinition that =
        (DaitchMokotoffSoundexTokenFilterDefinition) o;

    return this.outputOption == that.outputOption;
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.outputOption);
  }
}
