package com.xgen.mongot.embedding;

import com.xgen.mongot.util.bson.Vector;
import java.util.Objects;
import java.util.Optional;

public class VectorOrError {
  public static final VectorOrError EMPTY_INPUT_ERROR = new VectorOrError("Input token is empty");
  public final Optional<Vector> vector;
  // Contains Error specific to individual input, not general transient or non-transient error,
  // usually this is error that can be found in local sanity check
  public final Optional<String> errorMessage;

  public VectorOrError(Vector vector) {
    this.vector = Optional.of(vector);
    this.errorMessage = Optional.empty();
  }

  public VectorOrError(String errorMessage) {
    this.vector = Optional.empty();
    this.errorMessage = Optional.of(errorMessage);
  }

  @Override
  public boolean equals(Object that) {
    if (this == that) {
      return true;
    }
    if (that == null || !that.getClass().equals(VectorOrError.class)) {
      return false;
    }
    VectorOrError thatVector = (VectorOrError) that;
    return this.errorMessage.equals(thatVector.errorMessage)
        && this.vector.equals(thatVector.vector);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.vector, this.errorMessage);
  }

  @Override
  public String toString() {
    return "VectorOrError(vector=" + this.vector + ", errorMessage=" + this.errorMessage + ")";
  }
}
