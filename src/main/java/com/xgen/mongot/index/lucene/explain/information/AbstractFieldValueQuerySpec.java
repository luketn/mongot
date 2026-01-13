package com.xgen.mongot.index.lucene.explain.information;

import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.FieldPathField;
import java.util.Comparator;
import java.util.Objects;
import org.apache.commons.collections4.Equator;
import org.bson.BsonDocument;

/** Template for explaining queries of the form: Query(field, value). */
public abstract class AbstractFieldValueQuerySpec implements LuceneQuerySpecification {
  static class Fields {
    static final Field.Required<FieldPath> PATH =
        Field.builder("path").classField(FieldPathField::parse, FieldPathField::encode).required();

    static final Field.Required<String> VALUE = Field.builder("value").stringField().required();
  }

  private final Type type;
  private final FieldPath path;
  private final String value;

  protected AbstractFieldValueQuerySpec(Type type, FieldPath path, String value) {
    this.type = type;
    this.path = path;
    this.value = value;
  }

  /** Converts a lucene field name to a FieldPath representing the dotted path the user provides. */
  protected static FieldPath strip(String luceneFieldName) {
    return FieldPath.parse(FieldName.stripAnyPrefixFromLuceneFieldName(luceneFieldName));
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(TermQuerySpec.Fields.PATH, this.path)
        .field(TermQuerySpec.Fields.VALUE, this.value)
        .build();
  }

  @Override
  public Type getType() {
    return this.type;
  }

  @Override
  public boolean equals(
      LuceneQuerySpecification other,
      Equator<QueryExplainInformation> explainInfoEquator,
      Comparator<QueryExplainInformation> childSorter) {
    if (other.getType() != getType()) {
      return false;
    }

    AbstractFieldValueQuerySpec query = (AbstractFieldValueQuerySpec) other;
    return Objects.equals(this.path, query.path) && Objects.equals(this.value, query.value);
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    AbstractFieldValueQuerySpec that = (AbstractFieldValueQuerySpec) o;
    return this.type == that.type && this.path.equals(that.path) && this.value.equals(that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.type, this.path, this.value);
  }
}
