package com.xgen.testing.mongot.index.ingestion.serialization;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Objects;
import org.apache.lucene.document.ShapeField;
import org.apache.lucene.index.IndexableField;
import org.bson.BsonDocument;

public class LuceneIndexedGeoShapeField extends LuceneIndexedFieldSpec {
  static class Fields {
    static final Field.Required<String> VALUE = Field.builder("value").stringField().required();
  }

  private final String value;

  public LuceneIndexedGeoShapeField(String value) {
    this.value = value;
  }

  static LuceneIndexedGeoShapeField fromBson(DocumentParser parser) throws BsonParseException {
    return new LuceneIndexedGeoShapeField(parser.getField(Fields.VALUE).unwrap());
  }

  static LuceneIndexedGeoShapeField fromField(IndexableField field) {
    ShapeField.DecodedTriangle decodedTriangle = new ShapeField.DecodedTriangle();
    ShapeField.decodeTriangle(field.binaryValue().bytes, decodedTriangle);
    return new LuceneIndexedGeoShapeField(decodedTriangle.toString());
  }

  @Override
  BsonDocument fieldToBson() {
    return BsonDocumentBuilder.builder().field(Fields.VALUE, this.value).build();
  }

  @Override
  Type getType() {
    return Type.GEO_SHAPE;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    LuceneIndexedGeoShapeField that = (LuceneIndexedGeoShapeField) o;
    return this.value.equals(that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.value);
  }
}
