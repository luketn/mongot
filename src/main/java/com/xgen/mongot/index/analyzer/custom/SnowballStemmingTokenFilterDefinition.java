package com.xgen.mongot.index.analyzer.custom;

import com.xgen.mongot.index.analyzer.attributes.TokenStreamTransformationStage;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Objects;
import org.bson.BsonDocument;

public class SnowballStemmingTokenFilterDefinition extends TokenFilterDefinition
    implements TokenStreamTransformationStage.OutputsSameTypeAsInput {
  static class Fields {
    static final Field.Required<StemmerName> STEMMER_NAME =
        Field.builder("stemmerName").enumField(StemmerName.class).asCamelCase().required();
  }

  public enum StemmerName {
    ARABIC,
    ARMENIAN,
    BASQUE,
    CATALAN,
    DANISH,
    DUTCH,
    ENGLISH,
    ESTONIAN,
    FINNISH,
    FRENCH,
    GERMAN,
    GERMAN2,
    HUNGARIAN,
    IRISH,
    ITALIAN,
    LITHUANIAN,
    NORWEGIAN,
    PORTER,
    PORTUGUESE,
    ROMANIAN,
    RUSSIAN,
    SPANISH,
    SWEDISH,
    TURKISH
  }

  public final StemmerName stemmerName;

  public SnowballStemmingTokenFilterDefinition(StemmerName stemmerName) {
    this.stemmerName = stemmerName;
  }

  public static SnowballStemmingTokenFilterDefinition fromBson(DocumentParser parser)
      throws BsonParseException {
    return new SnowballStemmingTokenFilterDefinition(parser.getField(Fields.STEMMER_NAME).unwrap());
  }

  @Override
  public Type getType() {
    return Type.SNOWBALL_STEMMING;
  }

  @Override
  BsonDocument tokenFilterToBson() {
    return BsonDocumentBuilder.builder().field(Fields.STEMMER_NAME, this.stemmerName).build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SnowballStemmingTokenFilterDefinition that = (SnowballStemmingTokenFilterDefinition) o;
    return this.stemmerName == that.stemmerName;
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.stemmerName);
  }
}
