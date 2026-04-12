package com.xgen.mongot.index.analyzer.custom;

import com.xgen.mongot.index.analyzer.attributes.TokenStreamTransformationStage;
import java.util.Objects;
import org.bson.BsonDocument;

/**
 * The English Minimal Stem token filter stems words to their root for the English language. It only
 * handles removal of English plurals and a few other common suffixes, making it less aggressive
 * than other English stemmers (e.g. KStem, PorterStem) that also modify non-plural words.
 */
public class EnglishMinimalStemmingTokenFilterDefinition extends TokenFilterDefinition
    implements TokenStreamTransformationStage.OutputsSameTypeAsInput {

  @Override
  public Type getType() {
    return Type.ENGLISH_MINIMAL_STEMMING;
  }

  @Override
  BsonDocument tokenFilterToBson() {
    return new BsonDocument();
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof EnglishMinimalStemmingTokenFilterDefinition;
  }

  @Override
  public int hashCode() {
    return Objects.hash();
  }
}
