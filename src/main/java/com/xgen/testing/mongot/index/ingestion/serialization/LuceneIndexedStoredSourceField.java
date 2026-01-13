package com.xgen.testing.mongot.index.ingestion.serialization;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Objects;
import org.apache.lucene.index.IndexableField;
import org.bson.BsonDocument;
import org.bson.RawBsonDocument;

public class LuceneIndexedStoredSourceField extends LuceneIndexedFieldSpec {

  static class Fields {
    static final Field.Required<BsonDocument> VALUE =
        Field.builder("value").documentField().required();
  }

  private final BsonDocument value;

  public LuceneIndexedStoredSourceField(BsonDocument value) {
    this.value = value;
  }

  static LuceneIndexedStoredSourceField fromBson(DocumentParser parser) throws BsonParseException {
    return new LuceneIndexedStoredSourceField(parser.getField(Fields.VALUE).unwrap());
  }

  static LuceneIndexedStoredSourceField fromField(IndexableField field) {
    RawBsonDocument value = new RawBsonDocument(field.binaryValue().bytes);
    return new LuceneIndexedStoredSourceField(value.asDocument());
  }

  @Override
  BsonDocument fieldToBson() {
    return BsonDocumentBuilder.builder().field(Fields.VALUE, this.value).build();
  }

  @Override
  Type getType() {
    return Type.STORED_SOURCE;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    LuceneIndexedStoredSourceField that = (LuceneIndexedStoredSourceField) o;
    return this.value.equals(that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.value);
  }
}
