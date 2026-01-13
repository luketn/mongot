package com.xgen.mongot.index.path.string;

public final class UnresolvedStringWildcardPath extends UnresolvedStringPath {

  private final String value;

  public UnresolvedStringWildcardPath(String value) {
    this.value = value;
  }

  public String getValue() {
    return this.value;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }

    if (!(other instanceof UnresolvedStringWildcardPath)) {
      return false;
    }

    return this.value.equals(((UnresolvedStringWildcardPath) other).getValue());
  }

  @Override
  public int hashCode() {
    return this.value.hashCode();
  }

  @Override
  public String toString() {
    return this.value;
  }
}
