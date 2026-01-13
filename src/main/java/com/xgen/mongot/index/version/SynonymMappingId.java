package com.xgen.mongot.index.version;

import java.util.Objects;

/**
 * A synonym mapping identifier that is unique per mongot. Useful to uniquely identify a synonym
 * mapping across indexes and index generations.
 */
public class SynonymMappingId {
  public final GenerationId indexGenerationId;
  public final String name;

  private SynonymMappingId(GenerationId indexGenerationId, String name) {
    this.indexGenerationId = indexGenerationId;
    this.name = name;
  }

  public static SynonymMappingId from(GenerationId generationId, String name) {
    return new SynonymMappingId(generationId, name);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SynonymMappingId that = (SynonymMappingId) o;
    return this.indexGenerationId.equals(that.indexGenerationId) && this.name.equals(that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.indexGenerationId, this.name);
  }

  /** A string used to uniquely identify SynonymMappingId. */
  public String uniqueString() {
    return String.format("%s-%s", this.indexGenerationId.uniqueString(), this.name);
  }

  @Override
  public String toString() {
    return uniqueString();
  }
}
