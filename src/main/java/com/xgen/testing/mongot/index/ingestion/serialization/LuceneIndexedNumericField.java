package com.xgen.testing.mongot.index.ingestion.serialization;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Objects;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.bson.BsonDocument;

public class LuceneIndexedNumericField extends LuceneIndexedFieldSpec {

  static class Fields {
    static final Field.Required<Long> VALUE = Field.builder("value").longField().required();

    static final Field.Required<Boolean> HAS_POINT =
        Field.builder("hasPoint").booleanField().required();

    static final Field.WithDefault<Boolean> STORED =
        Field.builder("stored").booleanField().optional().withDefault(false);

    static final Field.WithDefault<DocValuesType> DOC_VALUES_TYPE =
        Field.builder("docValuesType")
            .enumField(DocValuesType.class)
            .asCamelCase()
            .optional()
            .withDefault(DocValuesType.NONE);
  }

  private final long value;
  private final boolean hasPoint;
  private final boolean stored;
  private final DocValuesType docValuesType;

  public LuceneIndexedNumericField(
      long value, boolean hasPoint, boolean stored, DocValuesType docValuesType) {
    this.value = value;
    this.hasPoint = hasPoint;
    this.stored = stored;
    this.docValuesType = docValuesType;
  }

  static LuceneIndexedNumericField fromBson(DocumentParser parser) throws BsonParseException {
    return new LuceneIndexedNumericField(
        parser.getField(Fields.VALUE).unwrap(),
        parser.getField(Fields.HAS_POINT).unwrap(),
        parser.getField(Fields.STORED).unwrap(),
        parser.getField(Fields.DOC_VALUES_TYPE).unwrap());
  }

  static LuceneIndexedNumericField fromField(IndexableField field) {
    long value = field.numericValue().longValue();
    boolean hasPoint = field.fieldType().pointDimensionCount() > 0;
    boolean stored = field.fieldType().stored();
    return new LuceneIndexedNumericField(
        value, hasPoint, stored, field.fieldType().docValuesType());
  }

  @Override
  BsonDocument fieldToBson() {
    var builder =
        BsonDocumentBuilder.builder()
            .field(Fields.VALUE, this.value)
            .field(Fields.HAS_POINT, this.hasPoint)
            .fieldOmitDefaultValue(Fields.DOC_VALUES_TYPE, this.docValuesType)
            .fieldOmitDefaultValue(Fields.STORED, this.stored);

    return builder.build();
  }

  @Override
  Type getType() {
    return Type.NUMERIC;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    LuceneIndexedNumericField that = (LuceneIndexedNumericField) o;
    return this.value == that.value
        && this.hasPoint == that.hasPoint
        && this.stored == that.stored
        && this.docValuesType == that.docValuesType;
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.value, this.hasPoint, this.stored, this.docValuesType);
  }
}
