package com.xgen.mongot.index.path.string;

import com.xgen.mongot.util.FieldPath;

public final class UnresolvedStringFieldPath extends UnresolvedStringPath {

  private final FieldPath value;

  public UnresolvedStringFieldPath(FieldPath value) {
    this.value = value;
  }

  public FieldPath getValue() {
    return this.value;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }

    if (!(other instanceof UnresolvedStringFieldPath)) {
      return false;
    }

    return this.value.equals(((UnresolvedStringFieldPath) other).value);
  }

  @Override
  public int hashCode() {
    return this.value.hashCode();
  }

  @Override
  public String toString() {
    return this.value.toString();
  }
}
