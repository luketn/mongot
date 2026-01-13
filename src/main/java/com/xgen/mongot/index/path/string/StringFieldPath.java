package com.xgen.mongot.index.path.string;

import com.xgen.mongot.util.FieldPath;

public class StringFieldPath extends StringPath {

  private final FieldPath value;

  public StringFieldPath(FieldPath value) {
    this.value = value;
  }

  @Override
  public Type getType() {
    return Type.FIELD;
  }

  public FieldPath getValue() {
    return this.value;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }

    if (!(other instanceof StringFieldPath)) {
      return false;
    }

    return this.value.equals(((StringFieldPath) other).value);
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
