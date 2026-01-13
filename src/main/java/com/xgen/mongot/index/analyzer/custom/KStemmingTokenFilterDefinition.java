package com.xgen.mongot.index.analyzer.custom;

import com.xgen.mongot.index.analyzer.attributes.TokenStreamTransformationStage;
import java.util.Objects;
import org.bson.BsonDocument;

/**
 * The K Stem token filter stems words to its root for the english language. It is a less aggressive
 * stemmer so in some cases it will skip over stemming plural words (i.e. ending in -s) based on the
 * rules it has.
 */
public class KStemmingTokenFilterDefinition extends TokenFilterDefinition
    implements TokenStreamTransformationStage.OutputsSameTypeAsInput {

  @Override
  public Type getType() {
    return Type.K_STEMMING;
  }

  @Override
  BsonDocument tokenFilterToBson() {
    return new BsonDocument();
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof KStemmingTokenFilterDefinition;
  }

  @Override
  public int hashCode() {
    return Objects.hash();
  }
}
