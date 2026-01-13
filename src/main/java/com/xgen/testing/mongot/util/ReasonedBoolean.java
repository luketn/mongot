package com.xgen.testing.mongot.util;

import java.util.Arrays;
import java.util.Objects;

public final class ReasonedBoolean {

  public static final String EMPTY = "";
  private static final String SEPARATOR = "\n";

  private final boolean isTrue;
  private final String falseReason;

  public static ReasonedBoolean ofFalse(String falseReason) {
    return new ReasonedBoolean(falseReason, false);
  }

  public static ReasonedBoolean ofTrue() {
    return new ReasonedBoolean(EMPTY, true);
  }

  public static ReasonedBoolean of(String falseReason, boolean isTrue) {
    return new ReasonedBoolean(isTrue ? EMPTY : falseReason, isTrue);
  }

  private ReasonedBoolean(String falseReason, boolean isTrue) {
    this.falseReason = falseReason;
    this.isTrue = isTrue;
  }

  public ReasonedBoolean or(ReasonedBoolean that) {
    boolean combinedIsTrue = this.isTrue || that.isTrue;
    return new ReasonedBoolean(
        combinedIsTrue ? EMPTY : combineReasons(this.falseReason, that.falseReason),
        combinedIsTrue);
  }

  public ReasonedBoolean and(ReasonedBoolean that) {
    boolean combinedIsTrue = this.isTrue && that.isTrue;
    return new ReasonedBoolean(
        combinedIsTrue ? EMPTY : combineReasons(this.falseReason, that.falseReason),
        combinedIsTrue);
  }

  public boolean isTrue() {
    return this.isTrue;
  }

  public String falseReason() {
    return this.falseReason;
  }

  @Override
  public String toString() {
    return "ReasonedBoolean["
        + "value="
        + this.isTrue
        + ", "
        + "falseReason="
        + this.falseReason
        + ']';
  }

  private static String combineReasons(String... reasons) {
    StringBuilder builder = new StringBuilder();
    Arrays.stream(reasons)
        .filter(Objects::nonNull)
        .filter(s -> !s.isBlank())
        .forEach(str -> builder.append(str).append(SEPARATOR));

    return builder.isEmpty() ? EMPTY : builder.toString();
  }
}
