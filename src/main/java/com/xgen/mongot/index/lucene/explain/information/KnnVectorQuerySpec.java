package com.xgen.mongot.index.lucene.explain.information;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Comparator;
import java.util.Objects;
import org.apache.commons.collections4.Equator;
import org.bson.BsonDocument;

public abstract class KnnVectorQuerySpec implements LuceneQuerySpecification {

  static class Fields {

    static final Field.Required<String> FIELD = Field.builder("field").stringField().required();

    static final Field.Required<Integer> K = Field.builder("k").intField().required();
  }

  final String field;
  final int k;

  public KnnVectorQuerySpec(String field, int k) {
    this.field = field;
    this.k = k;
  }

  @Override
  public boolean equals(
      LuceneQuerySpecification other,
      Equator<QueryExplainInformation> explainInfoEquator,
      Comparator<QueryExplainInformation> childSorter) {
    if (other.getType() != getType()) {
      return false;
    }
    KnnVectorQuerySpec otherQuerySpec = (KnnVectorQuerySpec) other;
    return Objects.equals(this.field, otherQuerySpec.field)
        && Objects.equals(this.k, otherQuerySpec.k);
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    KnnVectorQuerySpec that = (KnnVectorQuerySpec) o;
    return this.k == that.k && this.field.equals(that.field);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getType(), this.field, this.k);
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.FIELD, this.field)
        .field(Fields.K, this.k)
        .build();
  }
}
