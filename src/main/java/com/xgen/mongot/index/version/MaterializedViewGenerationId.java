package com.xgen.mongot.index.version;

import java.util.Objects;
import org.bson.types.ObjectId;

public class MaterializedViewGenerationId extends GenerationId {

  // Explicitly overrides super.generation here to avoid casting
  public final MaterializedViewGeneration generation;

  public MaterializedViewGenerationId(ObjectId indexId, MaterializedViewGeneration generation) {
    super(indexId, generation);
    this.generation = generation;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MaterializedViewGenerationId id = (MaterializedViewGenerationId) o;
    return this.indexId.equals(id.indexId) && this.generation.equals(id.generation);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.indexId, this.generation);
  }

  /** A string used to uniquely identify GenerationId. */
  @Override
  public String uniqueString() {
    // Materialized View only cares about user version.
    return String.format(
        "matview-%s-u%s",
        this.indexId.toHexString(), this.generation.userIndexVersion.versionNumber);
  }

  @Override
  public String toString() {
    return uniqueString();
  }
}
