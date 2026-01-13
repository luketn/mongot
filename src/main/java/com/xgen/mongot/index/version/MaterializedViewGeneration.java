package com.xgen.mongot.index.version;

import com.google.common.annotations.VisibleForTesting;
import java.util.Objects;
import org.bson.types.ObjectId;

/**
 * MaterializedViewGeneration represents generation for Materialized View in auto-embedding index
 */
public class MaterializedViewGeneration extends Generation {

  public MaterializedViewGeneration(Generation generation) {
    super(generation.userIndexVersion, IndexFormatVersion.CURRENT);
  }

  // This method should only be used in tests.
  @Override
  @VisibleForTesting
  public MaterializedViewGeneration incrementUser() {
    return new MaterializedViewGeneration(
        new Generation(this.userIndexVersion.increment(), IndexFormatVersion.CURRENT));
  }

  @Override
  public MaterializedViewGeneration nextAttempt() {
    return this;
  }

  @Override
  public MaterializedViewGenerationId generationId(ObjectId indexId) {
    return new MaterializedViewGenerationId(indexId, this);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof MaterializedViewGeneration)) {
      return false;
    }
    MaterializedViewGeneration that = (MaterializedViewGeneration) o;
    return this.userIndexVersion.equals(that.userIndexVersion);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.userIndexVersion);
  }

  @Override
  public String toString() {
    return String.format(
        "MaterializedViewGeneration{user=%d}", this.userIndexVersion.versionNumber);
  }
}
