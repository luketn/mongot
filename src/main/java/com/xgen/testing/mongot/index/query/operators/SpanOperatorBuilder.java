package com.xgen.testing.mongot.index.query.operators;

public abstract class SpanOperatorBuilder {

  public static SpanContainsOperatorBuilder contains() {
    return new SpanContainsOperatorBuilder();
  }

  public static SpanFirstOperatorBuilder first() {
    return new SpanFirstOperatorBuilder();
  }

  public static SpanNearOperatorBuilder near() {
    return new SpanNearOperatorBuilder();
  }

  public static SpanOrOperatorBuilder or() {
    return new SpanOrOperatorBuilder();
  }

  public static SpanTermOperatorBuilder term() {
    return new SpanTermOperatorBuilder();
  }
}
