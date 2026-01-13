package com.xgen.mongot.index;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Objects;
import org.bson.BsonDocument;
import org.bson.BsonValue;

public class FacetBucket implements DocumentEncodable {
  private static class Fields {
    static final Field.Required<BsonValue> ID =
        Field.builder("_id").unparsedValueField().required();

    static final Field.Required<Long> COUNT = Field.builder("count").longField().required();
  }

  private final BsonValue id;
  private final long count;

  public FacetBucket(BsonValue id, long count) {
    this.id = id;
    this.count = count;
  }

  static FacetBucket fromBson(DocumentParser parser) throws BsonParseException {
    return new FacetBucket(
        parser.getField(Fields.ID).unwrap(), parser.getField(Fields.COUNT).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.ID, this.id)
        .field(Fields.COUNT, this.count)
        .build();
  }

  public long getCount() {
    return this.count;
  }

  public BsonValue getId() {
    return this.id;
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.id, this.count);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    FacetBucket other = (FacetBucket) obj;
    return Objects.equals(this.id, other.id) && this.getCount() == other.getCount();
  }

  @Override
  public String toString() {
    return "FacetBucket(id=" + this.id + ", count=" + this.count + ")";
  }
}
