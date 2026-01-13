package com.xgen.mongot.index.lucene.query.values;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.util.CheckedStream;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.IndexSearcher;

public class MultiplyValuesSource extends DoubleValuesSource {
  private final List<DoubleValuesSource> args;

  private MultiplyValuesSource(List<DoubleValuesSource> args) {
    this.args = args;
  }

  public static MultiplyValuesSource create(List<DoubleValuesSource> args) {
    return new MultiplyValuesSource(args);
  }

  @Override
  public DoubleValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
    List<DoubleValues> argsDoubleValues =
        CheckedStream.from(this.args).mapAndCollectChecked(arg -> arg.getValues(ctx, scores));
    return new DoubleValues() {
      @Override
      public double doubleValue() throws IOException {
        @Var double product = 1;
        for (DoubleValues arg : argsDoubleValues) {
          product *= arg.doubleValue();
        }
        return product;
      }

      /**
       * Note: all arguments and their nested arguments are advanced to the doc regardless of
       * whether their advanceExact() methods return true/false.
       */
      @Override
      public boolean advanceExact(int doc) throws IOException {
        @Var boolean allTrue = true;
        for (DoubleValues arg : argsDoubleValues) {
          allTrue = arg.advanceExact(doc) && allTrue;
        }
        return allTrue;
      }
    };
  }

  @Override
  public boolean needsScores() {
    return this.args.stream().anyMatch(DoubleValuesSource::needsScores);
  }

  @Override
  public DoubleValuesSource rewrite(IndexSearcher reader) throws IOException {
    List<DoubleValuesSource> rewrittenArgs =
        CheckedStream.from(this.args).mapAndCollectChecked(arg -> arg.rewrite(reader));
    return new MultiplyValuesSource(rewrittenArgs);
  }

  @Override
  public String toString() {
    return String.format(
        "(%s)", this.args.stream().map(Object::toString).collect(Collectors.joining(" * ")));
  }

  /**
   * MultiplyValuesSource is only cache-able if all of its arguments are cache-able. This method
   * defines whether MultiplyValuesSource can be cached against the given LeafReaderContext's
   * LeafReader.
   */
  @Override
  public boolean isCacheable(LeafReaderContext ctx) {
    return this.args.stream().allMatch(arg -> arg.isCacheable(ctx));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof MultiplyValuesSource)) {
      return false;
    }
    MultiplyValuesSource that = (MultiplyValuesSource) o;
    return Objects.equals(this.args, that.args);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.args);
  }
}
