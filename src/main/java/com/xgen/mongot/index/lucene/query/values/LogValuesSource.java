package com.xgen.mongot.index.lucene.query.values;

import java.io.IOException;
import java.util.Objects;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.IndexSearcher;

public class LogValuesSource extends DoubleValuesSource {
  private final DoubleValuesSource argValue;
  private final boolean isLog1P;

  private LogValuesSource(DoubleValuesSource argValue, boolean isLog1P) {
    this.argValue = argValue;
    this.isLog1P = isLog1P;
  }

  /**
   * Use this method to create a LogValuesSource.
   *
   * @param argValue the value of the argument wrapped as a DoubleValuesSource
   * @param isLog1P whether this is a log1p expression or a log expression
   */
  public static LogValuesSource create(DoubleValuesSource argValue, boolean isLog1P) {
    return new LogValuesSource(argValue, isLog1P);
  }

  @Override
  public DoubleValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
    DoubleValues arg = this.argValue.getValues(ctx, scores);
    boolean isLog1P = this.isLog1P;
    return new DoubleValues() {
      @Override
      public double doubleValue() throws IOException {
        return isLog1P ? Math.log10(1 + arg.doubleValue()) : Math.log10(arg.doubleValue());
      }

      @Override
      public boolean advanceExact(int doc) throws IOException {
        return arg.advanceExact(doc);
      }
    };
  }

  @Override
  public boolean needsScores() {
    return this.argValue.needsScores();
  }

  @Override
  public DoubleValuesSource rewrite(IndexSearcher reader) throws IOException {
    return new LogValuesSource(this.argValue.rewrite(reader), this.isLog1P);
  }

  @Override
  public String toString() {
    String log = String.format("log(%s)", this.argValue);
    String log1P = String.format("log1p(%s)", this.argValue);
    return this.isLog1P ? log1P : log;
  }

  @Override
  public boolean isCacheable(LeafReaderContext ctx) {
    return this.argValue.isCacheable(ctx);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof LogValuesSource)) {
      return false;
    }
    LogValuesSource that = (LogValuesSource) o;
    return this.isLog1P == that.isLog1P && Objects.equals(this.argValue, that.argValue);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.argValue, this.isLog1P);
  }
}
