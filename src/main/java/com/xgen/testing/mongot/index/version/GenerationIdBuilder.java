package com.xgen.testing.mongot.index.version;

import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.index.version.UserIndexVersion;
import org.bson.types.ObjectId;

public class GenerationIdBuilder {

  /** convenience for a common case. */
  public static GenerationId create() {
    return new GenerationId(new ObjectId(), Generation.CURRENT);
  }

  /** convenience for a common case. */
  public static GenerationId create(ObjectId indexId) {
    return new GenerationId(indexId, Generation.CURRENT);
  }

  /** create a GenerationId. */
  public static GenerationId create(
      ObjectId indexId, int userVersion, int indexFormatVersion, int attempt) {
    return new GenerationId(
        indexId,
        new Generation(
            new UserIndexVersion(userVersion),
            IndexFormatVersion.create(indexFormatVersion),
            attempt));
  }

  /** Make the next generation as if a user modified the index definition. */
  public static GenerationId incrementUser(GenerationId before) {
    return new GenerationId(before.indexId, before.generation.incrementUser());
  }
}
