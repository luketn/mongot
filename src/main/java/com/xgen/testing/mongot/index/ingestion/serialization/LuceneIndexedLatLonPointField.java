package com.xgen.testing.mongot.index.ingestion.serialization;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Objects;
import org.apache.lucene.index.IndexableField;
import org.bson.BsonDocument;

public class LuceneIndexedLatLonPointField extends LuceneIndexedFieldSpec {
  static class Fields {
    static final Field.Required<String> VALUE = Field.builder("value").stringField().required();
  }

  private final String value;

  public LuceneIndexedLatLonPointField(String value) {
    this.value = value;
  }

  static LuceneIndexedLatLonPointField fromBson(DocumentParser parser) throws BsonParseException {
    return new LuceneIndexedLatLonPointField(parser.getField(Fields.VALUE).unwrap());
  }

  static LuceneIndexedLatLonPointField fromField(IndexableField field) {
    return new LuceneIndexedLatLonPointField(field.toString());
  }

  @Override
  BsonDocument fieldToBson() {
    return BsonDocumentBuilder.builder().field(Fields.VALUE, this.value).build();
  }

  @Override
  Type getType() {
    return Type.LAT_LON;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    LuceneIndexedLatLonPointField that = (LuceneIndexedLatLonPointField) o;
    return this.value.equals(that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.value);
  }
}
