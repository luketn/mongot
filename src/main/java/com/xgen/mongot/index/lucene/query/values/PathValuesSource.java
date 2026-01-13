package com.xgen.mongot.index.lucene.query.values;

import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import java.util.Objects;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.IndexSearcher;

public class PathValuesSource extends DoubleValuesSource {
  private final DoubleValuesSource fieldValue;
  private final double undefinedValue;
  private final FieldPath path;

  private PathValuesSource(DoubleValuesSource fieldValue, double undefinedValue, FieldPath path) {
    this.fieldValue = fieldValue;
    this.undefinedValue = undefinedValue;
    this.path = path;
  }

  /**
   * Use this method to create a PathValuesSource.
   *
   * @param fieldValue the value of the field wrapped as a DoubleValuesSource
   * @param undefinedValue the value to use if the field is not present in the document
   * @param path the path this PathValuesSource corresponds to
   */
  public static PathValuesSource create(
      DoubleValuesSource fieldValue, double undefinedValue, FieldPath path) {
    return new PathValuesSource(fieldValue, undefinedValue, path);
  }

  @Override
  public DoubleValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
    // Get the value of the field, or use the  if the field is not present.
    return DoubleValues.withDefault(this.fieldValue.getValues(ctx, scores), this.undefinedValue);
  }

  // Produced scorers do not need to know the relevancy scores of the matched documents, only the
  // values of fields in the documents.
  @Override
  public boolean needsScores() {
    return false;
  }

  @Override
  public DoubleValuesSource rewrite(IndexSearcher reader) throws IOException {
    return new PathValuesSource(this.fieldValue.rewrite(reader), this.undefinedValue, this.path);
  }

  @Override
  public String toString() {
    return this.path.toString();
  }

  @Override
  public boolean isCacheable(LeafReaderContext ctx) {
    return this.fieldValue.isCacheable(ctx);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PathValuesSource that = (PathValuesSource) o;
    return Double.compare(that.undefinedValue, this.undefinedValue) == 0
        && this.fieldValue.equals(that.fieldValue);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.fieldValue, this.undefinedValue);
  }
}
